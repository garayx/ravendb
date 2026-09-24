using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>
/// One planning conversation: the attachments that describe the source schema and the action
/// handlers that mutate the plan.
/// </summary>
public sealed class MigrationSession
{
    private static readonly TimeSpan ConversationLifetime = TimeSpan.FromDays(30);

    private readonly MigrationPlanStore _plans;
    private readonly IPlanChannel _channel;
    private readonly IAiConversationOperations _chat;
    private readonly string _slug;

    private CancellationToken _turnToken;

    public MigrationPlan Plan { get; }
    public SchemaCatalog Schema { get; }
    public string ConversationId => _chat.Id;

    private MigrationSession(
        MigrationPlanStore plans,
        IPlanChannel channel,
        IAiConversationOperations chat,
        string slug,
        MigrationPlan plan,
        SchemaCatalog schema)
    {
        _plans = plans;
        _channel = channel;
        _chat = chat;
        _slug = slug;
        Plan = plan;
        Schema = schema;
        RegisterHandlers();
    }

    /// <summary>
    /// Open a new conversation over the schema, with one DDL attachment per table for its first turn.
    /// </summary>
    public static MigrationSession Start(
        IDocumentStore store,
        MigrationPlanStore plans,
        IPlanChannel channel,
        string slug,
        SchemaCatalog schema)
    {
        var chat = OpenConversation(store, "MigrationChats/");

        foreach (var file in schema.Files)
            chat.AddAttachment(file.Name, file.OpenRead(), "text/plain");

        return new MigrationSession(plans, channel, chat, slug, new MigrationPlan(), schema);
    }

    /// <summary>
    /// Carry on an existing conversation with the plan it has registered so far. Without the plan the
    /// next turn would validate against nothing and re-register collections the user can already see.
    /// </summary>
    public static async Task<MigrationSession> ContinueAsync(
        IDocumentStore store,
        MigrationPlanStore plans,
        IPlanChannel channel,
        string slug,
        SchemaCatalog schema,
        string conversationId,
        CancellationToken token = default)
    {
        var plan = new MigrationPlan();

        if (await plans.LoadAsync(conversationId, token) is { } state)
            plan.Restore(state);

        return new MigrationSession(plans, channel, OpenConversation(store, conversationId), slug, plan, schema);
    }

    public async Task<MigrationReply> AskAsync(string prompt, CancellationToken token = default)
    {
        _chat.SetUserPrompt(prompt);
        _turnToken = token;

        var result = await _chat.RunAsync<MigrationReply>(token);
        await PersistAsync(token);
        return result.Answer;
    }

    // -----------------------------------------------------------------------
    // Action handlers
    // -----------------------------------------------------------------------

    private void HandleTool<TArgs>(string actionName, Func<TArgs, Task<ActionAck>> handler) where TArgs : class
    {
        _chat.Handle<string, ActionAck>(actionName, async raw =>
        {
            if (ToolArguments.TryRead<TArgs>(raw, out var args, out var error))
                return await handler(args);

            _channel.Note($"{actionName} was called with arguments that could not be read: {error}");

            return new ActionAck
            {
                Status = "rejected",
                Errors = [error],
                Registered = Plan.CollectionNames(),
                Next = $"Nothing was done. Fix the arguments to match the {actionName} schema and call it again."
            };
        });
    }

    private void RegisterHandlers()
    {
        HandleTool(SchemaMigrationAgentDefinition.ProposePlan, (ProposePlanArgs args) =>
        {
            _channel.ProposalRegistered(args);

            return Task.FromResult(new ActionAck
            {
                Status = "recorded",
                Registered = args.Collections.Select(c => c.Collection).ToArray()!,
                Next = "Wait for the user to choose which collections to build. Nothing is configured yet."
            });
        });

        HandleTool(SchemaMigrationAgentDefinition.AddCollection, async (AddCollectionArgs args) =>
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

        HandleTool(SchemaMigrationAgentDefinition.RemoveCollection, async (RemoveCollectionArgs args) =>
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

        HandleTool(SchemaMigrationAgentDefinition.SetConventions, async (SetConventionsArgs args) =>
        {
            if (PropertyCases.TryParse(args.PropertyCase, out var propertyCase) == false)
            {
                return new ActionAck
                {
                    Status = "rejected",
                    Errors = [$"Unknown PropertyCase '{args.PropertyCase}'. Use one of: {string.Join(", ", PropertyCases.Names)}."],
                    Registered = Plan.CollectionNames()
                };
            }

            var conventions = new NamingConventions(propertyCase, args.PropertyLanguage, args.Notes);
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

    private Task PersistAsync(CancellationToken token) => _plans.SaveAsync(_slug, ConversationId, Plan, token);

    private static IAiConversationOperations OpenConversation(IDocumentStore store, string conversationId) =>
        store.AI.Conversation(
            agentId: SchemaMigrationAgentDefinition.Identifier,
            conversationId: conversationId,
            creationOptions: new AiConversationCreationOptions
            {
                ExpirationInSec = (int)ConversationLifetime.TotalSeconds
            });
}
