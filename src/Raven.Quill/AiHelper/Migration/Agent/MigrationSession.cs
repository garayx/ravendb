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
    private static readonly TimeSpan ConversationLifetime = TimeSpan.FromDays(30);

    private readonly IDocumentStore _store;
    private readonly MigrationPlanStore _plans;
    private readonly IPlanChannel _channel;
    private readonly IAiConversationOperations _chat;
    private readonly string _slug;
    private readonly List<string> _prompts = new();
    private readonly List<Stream> _pendingAttachments = new();

    private CancellationToken _turnToken;

    public MigrationPlan Plan { get; }
    public SchemaCatalog Schema { get; }
    public string ConversationId => _chat.Id;

    private MigrationSession(
        IDocumentStore store,
        MigrationPlanStore plans,
        IPlanChannel channel,
        IAiConversationOperations chat,
        string slug,
        MigrationPlan plan,
        SchemaCatalog schema)
    {
        _store = store;
        _plans = plans;
        _channel = channel;
        _chat = chat;
        _slug = slug;
        Plan = plan;
        Schema = schema;
        RegisterHandlers();
    }

    /// <summary>
    /// Start a session over a schema. Pass a conversationId prefix ending in '/' for a new
    /// conversation, or a full ID to continue an existing one.
    /// </summary>
    public static MigrationSession Start(
        IDocumentStore store,
        MigrationPlanStore plans,
        IPlanChannel channel,
        string slug,
        SchemaCatalog schema,
        string conversationId = "MigrationChats/")
    {
        var chat = OpenConversation(store, conversationId);

        return new MigrationSession(store, plans, channel, chat, slug, new MigrationPlan(), schema);
    }

    public static MigrationSession Start(
        IDocumentStore store,
        MigrationPlanStore plans,
        IPlanChannel channel,
        string slug,
        IEnumerable<string> ddlPaths,
        string conversationId = "MigrationChats/") =>
        Start(store, plans, channel, slug, SchemaCatalog.FromFiles(ddlPaths), conversationId);

    /// <summary>
    /// Continue an existing conversation, rehydrating everything it has already registered. Without
    /// this a second request would drive the same conversation against an empty plan and re-register
    /// collections the user can already see.
    /// </summary>
    public static async Task<MigrationSession> ResumeAsync(
        IDocumentStore store,
        MigrationPlanStore plans,
        IPlanChannel channel,
        string slug,
        SchemaCatalog schema,
        string conversationId,
        CancellationToken token = default)
    {
        var session = Start(store, plans, channel, slug, schema, conversationId);
        var state = await plans.LoadAsync(conversationId, token);

        if (state is not null)
        {
            session.Plan.Restore(state);
            session._prompts.AddRange(state.Prompts);
        }

        return session;
    }

    /// <summary>
    /// Attach the DDL files to the next turn. Attachments are queued rather than read here, so the
    /// streams stay open until the turn has run and are disposed by <see cref="AskAsync"/>.
    /// </summary>
    public void AttachSchema()
    {
        foreach (var file in Schema.Files)
        {
            var stream = file.OpenRead();
            _pendingAttachments.Add(stream);
            _chat.AddAttachment(file.Name, stream, "text/plain");
        }
    }

    public async Task<MigrationReply> AskAsync(string prompt, CancellationToken token = default)
    {
        _prompts.Add(prompt);
        _chat.SetUserPrompt(prompt);
        _turnToken = token;

        try
        {
            var result = await _chat.RunAsync<MigrationReply>(token);
            await PersistAsync(token);
            return result.Answer;
        }
        finally
        {
            foreach (var stream in _pendingAttachments)
                stream.Dispose();

            _pendingAttachments.Clear();
        }
    }

    // -----------------------------------------------------------------------
    // Action handlers
    // -----------------------------------------------------------------------

    private void RegisterHandlers()
    {
        _chat.Handle(SchemaMigrationAgentDefinition.ProposePlan, async (ProposePlanArgs args) =>
        {
            Plan.SetProposal(JsonSerializer.SerializeToElement(args, JsonHelper.Options));
            _channel.ProposalRegistered(args);
            await PersistAsync(_turnToken);

            return new ActionAck
            {
                Status = "recorded",
                Registered = args.Collections.Select(c => c.Collection).ToArray()!,
                Next = "Wait for the user to choose which collections to build. Nothing is configured yet."
            };
        });

        _chat.Handle(SchemaMigrationAgentDefinition.AddCollection, async (AddCollectionArgs args) =>
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

            // Accepted: into the plan, persisted, and straight out to the user.
            var entry = Plan.Upsert(collection, args.Rationale, args.Config);
            await PersistAsync(_turnToken);
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

        _chat.Handle(SchemaMigrationAgentDefinition.RemoveCollection, async (RemoveCollectionArgs args) =>
        {
            var removed = Plan.Remove(args.Collection ?? string.Empty);
            if (removed)
            {
                await PersistAsync(_turnToken);
                _channel.CollectionRemoved(args.Collection, args.Reason);
            }

            return new ActionAck
            {
                Status = removed ? "removed" : "not_found",
                Collection = args.Collection,
                Registered = Plan.CollectionNames()
            };
        });

        _chat.Handle(SchemaMigrationAgentDefinition.SetConventions, async (SetConventionsArgs args) =>
        {
            var conventions = new NamingConventions(args.PropertyCase, args.PropertyLanguage, args.Notes);
            Plan.SetConventions(conventions);
            await PersistAsync(_turnToken);

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

    /// <summary>Publicly callable so a fork can record its owner before any turn has run.</summary>
    public Task PersistAsync(CancellationToken token) =>
        _plans.SaveAsync(_slug, ConversationId, Plan, InputKey(), _prompts, token);

    // -----------------------------------------------------------------------
    // Checkpoint and fork
    // -----------------------------------------------------------------------

    /// <summary>
    /// The key for the current state: agent, prompt version, attachment digests, prompts so far.
    /// Deterministic - the same schema and the same prompts produce the same key on any machine.
    /// </summary>
    public string InputKey() => PlanCheckpoint.ComputeInputKey(
        SchemaMigrationAgentDefinition.Identifier,
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
            Slug = _slug,
            SourceConversationId = ConversationId,
            CreatedAt = DateTime.UtcNow,
            Schema = Schema.Files,
            Prompts = _prompts.ToList(),
            ProposalJson = Plan.Proposal is { } p ? p.GetRawText() : null
        };

        var streams = new List<Stream>();

        try
        {
            using var session = _store.OpenAsyncSession();
            await session.StoreAsync(checkpoint, checkpoint.Id, token);

            foreach (var file in Schema.Files)
            {
                var stream = file.OpenRead();
                streams.Add(stream);
                session.Advanced.Attachments.Store(checkpoint.Id, file.Name, stream, "text/plain");
            }

            await session.SaveChangesAsync(token);
        }
        finally
        {
            foreach (var stream in streams)
                stream.Dispose();
        }

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
        MigrationPlanStore plans,
        IPlanChannel channel,
        string slug,
        PlanCheckpoint checkpoint,
        string branch,
        SchemaCatalog? schema = null)
    {
        var chat = OpenConversation(store, checkpoint.ConversationIdFor(branch));

        // The files are already in the database. Copy them into this turn rather than shipping
        // them from the client again.
        foreach (var file in checkpoint.Schema)
            chat.CopyAttachmentFrom(checkpoint.Id, file.Name);

        if (string.IsNullOrWhiteSpace(checkpoint.ProposalJson) == false)
        {
            chat.AddArtificialActionWithResponse(
                SchemaMigrationAgentDefinition.ProposePlan, checkpoint.ProposalJson);
        }

        // Validation needs the schema facts. A caller that still holds them passes them in, which
        // keeps the branch as exact as the session it came from; only a caller with nothing left
        // falls back to reading the DDL back off the checkpoint.
        var catalog = schema ?? CatalogFromCheckpoint(store, checkpoint);

        var session = new MigrationSession(store, plans, channel, chat, slug, new MigrationPlan(), catalog);
        session._prompts.AddRange(checkpoint.Prompts);

        if (MigrationPlan.ParseProposal(checkpoint.ProposalJson) is { } proposal)
            session.Plan.SetProposal(proposal);

        return session;
    }

    private static IAiConversationOperations OpenConversation(IDocumentStore store, string conversationId) =>
        store.AI.Conversation(
            agentId: SchemaMigrationAgentDefinition.Identifier,
            conversationId: conversationId,
            creationOptions: new AiConversationCreationOptions
            {
                // A planning session is worked on over days, not minutes.
                ExpirationInSec = (int)ConversationLifetime.TotalSeconds
            });

    /// <summary>
    /// Last resort for a fork whose caller no longer has the schema: write the checkpoint's DDL
    /// attachments out, index them, and delete them again. The facts this recovers are only as good
    /// as the DDL parses, so a caller that can pass the discovered schema should.
    /// </summary>
    private static SchemaCatalog CatalogFromCheckpoint(IDocumentStore store, PlanCheckpoint checkpoint)
    {
        if (checkpoint.Schema.All(f => string.IsNullOrEmpty(f.Path) == false && File.Exists(f.Path)))
            return SchemaCatalog.FromFiles(checkpoint.Schema.Select(f => f.Path!));

        var dir = Directory.CreateTempSubdirectory("rvn-migration-");

        try
        {
            var paths = new List<string>();

            using (var session = store.OpenSession())
            {
                foreach (var file in checkpoint.Schema)
                {
                    using var attachment = session.Advanced.Attachments.Get(checkpoint.Id, file.Name);
                    var path = Path.Combine(dir.FullName, file.Name);

                    using (var target = File.Create(path))
                        attachment.Stream.CopyTo(target);

                    paths.Add(path);
                }
            }

            return SchemaCatalog.FromFiles(paths);
        }
        finally
        {
            // The catalog holds its index in memory, so the files have done their job.
            dir.Delete(recursive: true);
        }
    }
}
