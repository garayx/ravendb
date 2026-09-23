using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>
/// One planning conversation: the attachments that describe the source schema, the action
/// handlers that mutate the plan, and the checkpoint that lets the whole thing be resumed
/// or forked without re-analysing the schema.
/// </summary>
public sealed class MigrationSession
{
    private readonly IDocumentStore _store;
    private readonly IPlanChannel _channel;
    private readonly IAiConversationOperations _chat;
    private readonly List<string> _prompts = new();

    public MigrationPlan Plan { get; }
    public SchemaCatalog Schema { get; }
    public string ConversationId => _chat.Id;

    private MigrationSession(
        IDocumentStore store,
        IPlanChannel channel,
        IAiConversationOperations chat,
        MigrationPlan plan,
        SchemaCatalog schema)
    {
        _store = store;
        _channel = channel;
        _chat = chat;
        Plan = plan;
        Schema = schema;
        RegisterHandlers();
    }

    /// <summary>
    /// Start a session over a set of DDL files. Pass a conversationId prefix ending in '/' for a
    /// new conversation, or a full ID to continue an existing one.
    /// </summary>
    public static MigrationSession Start(
        IDocumentStore store,
        IPlanChannel channel,
        IEnumerable<string> ddlPaths,
        string conversationId = "MigrationChats/")
    {
        var schema = SchemaCatalog.FromFiles(ddlPaths);

        var chat = store.AI.Conversation(
            agentId: SchemaMigrationAgentDefinition.Identifier,
            conversationId: conversationId,
            creationOptions: new AiConversationCreationOptions
            {
                // A planning session is worked on over days, not minutes.
                ExpirationInSec = (int)TimeSpan.FromDays(30).TotalSeconds
            });

        return new MigrationSession(store, channel, chat, new MigrationPlan(), schema);
    }

    /// <summary>
    /// Attach the DDL files to the next turn. Attachments are scoped to a single turn and cleared
    /// after RunAsync, which is fine here: the model summarises each file into the conversation
    /// context on the first turn, and the summary is what later turns reason over.
    /// </summary>
    public void AttachSchema()
    {
        foreach (var file in Schema.Files)
        {
            var stream = File.OpenRead(file.Path);
            _chat.AddAttachment(file.Name, stream, "text/plain");
        }
    }

    public async Task<MigrationReply> AskAsync(string prompt, CancellationToken token = default)
    {
        _prompts.Add(prompt);
        _chat.SetUserPrompt(prompt);

        var result = await _chat.RunAsync<MigrationReply>(token);
        return result.Answer;
    }

    // -----------------------------------------------------------------------
    // Action handlers
    // -----------------------------------------------------------------------

    private void RegisterHandlers()
    {
        _chat.Handle(SchemaMigrationAgentDefinition.ProposePlan, (ProposePlanArgs args) =>
        {
            Plan.SetProposal(JsonSerializer.SerializeToElement(args, Json.Options));
            _channel.ProposalRegistered(args);

            return new ActionAck
            {
                Status = "recorded",
                Registered = args.Collections.Select(c => c.Collection).ToArray()!,
                Next = "Wait for the user to choose which collections to build. Nothing is configured yet."
            };
        });

        _chat.Handle(SchemaMigrationAgentDefinition.AddCollection, (AddCollectionArgs args) =>
        {
            var collection = args.Collection ?? args.Config?.CollectionName;

            if (string.IsNullOrWhiteSpace(collection))
            {
                return ActionAck.Rejected("(unnamed)",
                    new[] { "Collection is required." }, Array.Empty<string>());
            }

            var validation = PlanValidator.Validate(collection, args.Config, Plan, Schema);

            if (validation.Ok == false)
            {
                // Nothing is registered. The errors go back to the model, and the user sees that
                // the attempt was rejected rather than a silent gap in the plan.
                _channel.CollectionRejected(collection, validation.Errors);
                return ActionAck.Rejected(collection, validation.Errors, validation.Warnings);
            }

            // Accepted: into the in-memory plan, and straight out to the user.
            var entry = Plan.Upsert(collection, args.Rationale, args.Config);
            _channel.CollectionRegistered(entry, validation.Warnings);

            return new ActionAck
            {
                Status = entry.Version == 1 ? "registered" : "replaced",
                Collection = collection,
                Version = entry.Version,
                Warnings = validation.Warnings.ToArray(),
                Registered = Plan.CollectionNames(),
                Next = "Registered and shown to the user. Do not repeat the configuration in your reply."
            };
        });

        _chat.Handle(SchemaMigrationAgentDefinition.RemoveCollection, (RemoveCollectionArgs args) =>
        {
            var removed = Plan.Remove(args.Collection ?? string.Empty);
            if (removed)
                _channel.CollectionRemoved(args.Collection, args.Reason);

            return new ActionAck
            {
                Status = removed ? "removed" : "not_found",
                Collection = args.Collection,
                Registered = Plan.CollectionNames()
            };
        });

        _chat.Handle(SchemaMigrationAgentDefinition.SetConventions, (SetConventionsArgs args) =>
        {
            var conventions = new NamingConventions(args.PropertyCase, args.PropertyLanguage, args.Notes);
            Plan.SetConventions(conventions);

            // Conventions do not rewrite what is already registered - the model has to re-emit,
            // and validation now rejects anything that does not conform. Tell it exactly what is
            // outstanding so it does not have to guess or ask.
            var mustReEmit = Plan.CollectionNames();
            _channel.ConventionsChanged(conventions, mustReEmit);

            return new ActionAck
            {
                Status = "recorded",
                Registered = mustReEmit,
                Next = mustReEmit.Length == 0
                    ? "No collections registered yet - apply these conventions to everything you emit."
                    : $"These collections still use the previous convention: {string.Join(", ", mustReEmit)}. " +
                      "Call add_collection again for each one with every property name translated and re-cased. " +
                      "Column names on the SQL side do not change. Validation will reject any property that " +
                      "does not conform."
            };
        });

        // An action tool with no registered handler throws by default. Keep the turn alive and
        // tell the model plainly, rather than letting one hallucinated tool name kill a session
        // that has real mappings in it.
        _chat.OnUnhandledAction += args =>
        {
            _channel.Note($"unhandled action tool '{args.Action.Name}' - no handler registered");
            args.Sender.AddActionResponse(
                args.Action.ToolId,
                $"There is no '{args.Action.Name}' tool. Use only propose_plan, add_collection, " +
                "remove_collection and set_conventions.");
            return Task.CompletedTask;
        };
    }

    // -----------------------------------------------------------------------
    // Checkpoint and fork
    // -----------------------------------------------------------------------

    /// <summary>
    /// The key for the current state: agent, prompt version, attachment digests, prompts so far.
    /// Deterministic - the same schema and the same prompts produce the same key on any machine.
    /// </summary>
    public string InputKey() => PlanCheckpoint.ComputeInputKey(
        SchemaMigrationAgentDefinition.Identifier,
        SchemaMigrationAgentDefinition.SystemPromptVersion,
        Schema.Files,
        _prompts);

    /// <summary>
    /// Store the current state as a checkpoint. The DDL files ride along as attachments on the
    /// checkpoint document, so a fork can pull them with CopyAttachmentFrom instead of the client
    /// re-uploading them.
    /// </summary>
    public async Task<PlanCheckpoint> CheckpointAsync(CancellationToken token = default)
    {
        var inputKey = InputKey();
        var checkpoint = new PlanCheckpoint
        {
            Id = PlanCheckpoint.DocumentId(inputKey),
            InputKey = inputKey,
            AgentIdentifier = SchemaMigrationAgentDefinition.Identifier,
            SystemPromptVersion = SchemaMigrationAgentDefinition.SystemPromptVersion,
            SourceConversationId = ConversationId,
            CreatedAt = DateTime.UtcNow,
            Schema = Schema.Files,
            Prompts = _prompts.ToList(),
            ProposalJson = Plan.Proposal is { } p ? p.GetRawText() : null
        };

        using var session = _store.OpenAsyncSession();
        await session.StoreAsync(checkpoint, checkpoint.Id, token);

        foreach (var file in Schema.Files)
        {
            session.Advanced.Attachments.Store(
                checkpoint.Id, file.Name, File.OpenRead(file.Path), "text/plain");
        }

        await session.SaveChangesAsync(token);
        return checkpoint;
    }

    public static async Task<PlanCheckpoint?> FindCheckpointAsync(
        IDocumentStore store,
        string inputKey,
        CancellationToken token = default)
    {
        using var session = store.OpenAsyncSession();
        return await session.LoadAsync<PlanCheckpoint>(PlanCheckpoint.DocumentId(inputKey), token);
    }

    /// <summary>
    /// Branch off a checkpoint. Creates a fresh conversation, pulls the schema attachments from
    /// the checkpoint document, and injects the stored propose_plan result so the model starts
    /// out believing it already analysed the schema. The branch gets a clean plan and a clean
    /// message history; the only thing it inherits is the analysis.
    /// </summary>
    public static MigrationSession Fork(
        IDocumentStore store,
        IPlanChannel channel,
        PlanCheckpoint checkpoint,
        string branch)
    {
        if (checkpoint.SystemPromptVersion != SchemaMigrationAgentDefinition.SystemPromptVersion)
        {
            throw new InvalidOperationException(
                $"Checkpoint {checkpoint.InputKey} was taken against system prompt v" +
                $"{checkpoint.SystemPromptVersion}; this build is v" +
                $"{SchemaMigrationAgentDefinition.SystemPromptVersion}. Re-run the analysis instead " +
                "of resuming against guidance the model was never given.");
        }

        var chat = store.AI.Conversation(
            agentId: SchemaMigrationAgentDefinition.Identifier,
            conversationId: checkpoint.ConversationIdFor(branch),
            creationOptions: new AiConversationCreationOptions
            {
                ExpirationInSec = (int)TimeSpan.FromDays(30).TotalSeconds
            });

        // The files are already in the database. Copy them into this turn rather than shipping
        // them from the client again.
        foreach (var file in checkpoint.Schema)
            chat.CopyAttachmentFrom(checkpoint.Id, file.Name);

        if (string.IsNullOrWhiteSpace(checkpoint.ProposalJson) == false)
        {
            chat.AddArtificialActionWithResponse(
                SchemaMigrationAgentDefinition.ProposePlan, checkpoint.ProposalJson);
        }

        // Local schema files are needed for validation. Prefer the recorded paths; if they are
        // gone, write the checkpoint's attachments to a temp directory instead.
        var paths = checkpoint.Schema.Select(f => f.Path).ToArray();
        var catalog = paths.All(File.Exists)
            ? SchemaCatalog.FromFiles(paths)
            : SchemaCatalog.FromFiles(MaterialiseAttachments(store, checkpoint));

        var session = new MigrationSession(store, channel, chat, new MigrationPlan(), catalog);
        session._prompts.AddRange(checkpoint.Prompts);

        if (string.IsNullOrWhiteSpace(checkpoint.ProposalJson) == false)
            session.Plan.SetProposal(JsonDocument.Parse(checkpoint.ProposalJson).RootElement);

        return session;
    }

    private static IEnumerable<string> MaterialiseAttachments(IDocumentStore store, PlanCheckpoint checkpoint)
    {
        var dir = Directory.CreateTempSubdirectory("rvn-migration-").FullName;
        using var session = store.OpenSession();

        foreach (var file in checkpoint.Schema)
        {
            using var attachment = session.Advanced.Attachments.Get(checkpoint.Id, file.Name);
            var path = Path.Combine(dir, file.Name);
            using var target = File.Create(path);
            attachment.Stream.CopyTo(target);
            yield return path;
        }
    }
}
