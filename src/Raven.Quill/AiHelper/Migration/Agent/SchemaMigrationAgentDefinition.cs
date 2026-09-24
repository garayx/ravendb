using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Quill.AiHelper.Migration.Planning;

namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>
/// Creates the schema migration planner agent: reads relational DDL from conversation
/// attachments and emits CDC Sink table configurations through action tools.
/// </summary>
public static class SchemaMigrationAgentDefinition
{
    public const string Identifier = "quill-schema-migration-planner-gpt-5-mini-ai-agent";

    public const string ConnectionStringName = "quill-open-ai-gpt-5-mini";

    public const string Model = "gpt-5-mini";

    public const string ProposePlan = "propose_plan";
    public const string AddCollection = "add_collection";
    public const string RemoveCollection = "remove_collection";
    public const string SetConventions = "set_conventions";

    public static async Task CreateOrUpdateAsync(IDocumentStore store, CancellationToken token = default)
    {
        var agent = new AiAgentConfiguration(
            name: "QuillSchemaMigrationPlannerGpt5MiniAiAgent",
            connectionStringName: ConnectionStringName,
            systemPrompt: SystemPrompt)
        {
            Identifier = Identifier,

            // A turn emits one add_collection per collection plus a retry apiece, so the ceiling
            // has to clear the whole schema. It only exists to stop a runaway loop, so it is set
            // well above anything a real plan needs rather than computed per conversation.
            MaxModelIterationsPerCall = 256,

            ChatTrimming = new AiAgentChatTrimmingConfiguration(
                new AiAgentSummarizationByTokens
                {
                    MaxTokensBeforeSummarization = 96_000,
                    MaxTokensAfterSummarization = 8_000,

                    // Default summarisation drops exactly what we need to keep. The mappings are
                    // the state of the work; losing a column list mid-conversation means the next
                    // correction turn silently re-invents it.
                    SummarizationTaskBeginningPrompt =
                        "Summarise this schema migration planning session. Preserve, in full: the source " +
                        "table and column names discussed, every collection already registered and the " +
                        "tables it absorbs, the naming conventions in force, and any gap the user was told " +
                        "about. Discard conversational filler and superseded drafts.",

                    SummarizationTaskEndPrompt =
                        "Summarise the conversation above under those rules. The mappings are the state of " +
                        "the work: a table or column name dropped here is one the next turn will re-invent.",

                    ResultPrefix = "Planning session so far:"
                },

                // Keep the original history. For a migration this is the design record - months
                // later, "why is Region copied onto the order?" has an answer.
                new AiAgentHistoryConfiguration()),

            SampleObject = JsonHelper.Pretty(new MigrationReply
            {
                Reply = "Explain the modelling decisions and their trade-offs. Do not repeat configuration JSON.",
                Gaps = new[] { "something the user asked for that the source schema cannot express" },
                OpenQuestions = new[]
                {
                    new MigrationOpenQuestion
                    {
                        Question = "a decision you want the user to make",
                        Options = new[]
                        {
                            new MigrationAnswerOption { Answer = "the answer you would pick", IsRecommended = true },
                            new MigrationAnswerOption { Answer = "a reasonable alternative", IsRecommended = false },
                            new MigrationAnswerOption { Answer = "another reasonable alternative", IsRecommended = false }
                        }
                    }
                }
            }),

            Actions = new List<AiAgentToolAction>
            {
                new()
                {
                    Name = ProposePlan,
                    Description =
                        "Call this exactly once, on the first turn, after reading the attached DDL. Groups the " +
                        "source tables into candidate collections and reports what each collection absorbs and " +
                        "how. The proposal is shown to the user to choose from. This tool does not " +
                        "create any mapping - nothing is configured until add_collection is called.",
                    ParametersSampleObject = JsonHelper.Pretty(SampleProposal)
                },
                new()
                {
                    Name = AddCollection,
                    Description =
                        "Register one root collection and its complete CDC Sink table configuration. Call once " +
                        "per collection, never once for several. The configuration is validated against the " +
                        "source schema and the agreed naming conventions; on rejection the response lists every " +
                        "error and nothing is registered, so fix them and call again. Registration is an upsert " +
                        "keyed by collection name: calling again for the same collection replaces its mapping.",
                    ParametersSampleObject = JsonHelper.Pretty(SampleAddCollection)
                },
                new()
                {
                    Name = RemoveCollection,
                    Description =
                        "Remove a previously registered collection from the plan, when the user drops it or " +
                        "when it turned out to belong inside another document.",
                    ParametersSampleObject = JsonHelper.Pretty(new RemoveCollectionArgs
                    {
                        Collection = "Products",
                        Reason = "why it is being removed"
                    })
                },
                new()
                {
                    Name = SetConventions,
                    Description =
                        "Record the property naming convention and language for the whole plan. Call this when " +
                        "the user states or changes them. Changing conventions does not rewrite mappings that " +
                        "are already registered - the response tells you which collections must be re-emitted " +
                        "through add_collection, and validation will then reject anything that does not conform. " +
                        "PropertyCase is one of SnakeCase, CamelCase or PascalCase.",
                    ParametersSampleObject = JsonHelper.Pretty(new SetConventionsArgs
                    {
                        PropertyCase = nameof(PropertyCase.SnakeCase),
                        PropertyLanguage = "Spanish",
                        Notes = "optional, e.g. keep identifiers and enum values untranslated"
                    })
                }
            }
        };

        await store.AI.CreateAgentAsync(agent, token);
    }

    // A worked example beats a schema. The model is shown a small but complete configuration
    // that exercises embed, link and a child-side patch, in the shape it must produce.
    private static AddCollectionArgs SampleAddCollection => new()
    {
        Collection = "Orders",
        Rationale = "Lines are owned by the order and always read with it; the customer has its own lifecycle.",
        Config = new CdcSinkTableConfig
        {
            CollectionName = "Orders",
            SourceTableName = "orders",
            SourceTableSchema = "public",
            PrimaryKeyColumns = new List<string> { "order_id" },
            Columns = new List<CdcColumnMapping>
            {
                new() { Column = "order_id", Name = "OrderId" },
                new() { Column = "ordered_at", Name = "OrderedAt" },
                new() { Column = "metadata", Name = "Metadata", Type = CdcColumnType.Json }
            },
            LinkedTables = new List<CdcSinkLinkedTableConfig>
            {
                new()
                {
                    SourceTableName = "customers",
                    PropertyName = "Customer",
                    LinkedCollectionName = "Customers",
                    JoinColumns = new List<string> { "customer_id" }
                }
            },
            EmbeddedTables = new List<CdcSinkEmbeddedTableConfig>
            {
                new()
                {
                    SourceTableName = "order_lines",
                    PropertyName = "Lines",
                    Type = CdcSinkRelationType.Array,
                    JoinColumns = new List<string> { "order_id" },
                    PrimaryKeyColumns = new List<string> { "order_line_id" },
                    Columns = new List<CdcColumnMapping>
                    {
                        new() { Column = "order_line_id", Name = "LineId" },
                        new() { Column = "quantity", Name = "Quantity" },
                        new() { Column = "unit_price", Name = "UnitPrice" }
                    },
                    LinkedTables = new List<CdcSinkLinkedTableConfig>
                    {
                        new()
                        {
                            SourceTableName = "products",
                            PropertyName = "Product",
                            LinkedCollectionName = "Products",
                            JoinColumns = new List<string> { "product_id" }
                        }
                    },
                    Patch = "this.Total = (this.Lines || []).reduce((s, l) => s + l.UnitPrice * l.Quantity, 0);"
                }
            }
        }
    };

    private static ProposePlanArgs SampleProposal => new()
    {
        Areas = new[]
        {
            new ProposedArea
            {
                Area = "Order capture",
                Collections = new[] { "Orders", "Products" },
                Why = "one bounded write path, read as a unit"
            }
        },
        Collections = new[]
        {
            new ProposedCollection
            {
                Collection = "Orders",
                RootTable = "orders",
                Absorbs = new[]
                {
                    new AbsorbedTable { Table = "order_lines", How = "Embed", Why = "owned by the order, bounded" },
                    new AbsorbedTable { Table = "customers", How = "Link", Why = "own lifecycle, queried on its own" }
                },
                Why = "the document the order screen loads in one request"
            }
        },
        Dropped = new[]
        {
            new DroppedTable { Table = "order_tags", Why = "pure join table - becomes an array of references" }
        },
        Enables = new[] { "an agent that can answer questions about an order without a join" }
    };

    private const string SystemPrompt = """
        You are a relational-to-document migration architect. The user attaches the DDL of a
        relational schema, one file per table. You decide how those tables become RavenDB
        documents, and you emit that decision as CDC Sink table configurations through your tools.

        CDC Sink follows the source database's change feed and keeps the documents synchronised.
        It is not a one-time import. Two consequences shape everything you do:

        - The mapping is a live projection. Values copied from a related table stay current;
          they are not snapshots. If the user needs a historical value, say so - that has to
          come from the source schema, not from the mapping.
        - Mappings apply forward only. Changing one later does not rewrite documents that
          already exist; the only way to reshape them is to recreate the task and re-run the
          initial load. So the shape has to be right before it is applied, not after.

        # How to model

        Start from the document the application will load, not from the tables. For every table
        related to a root, choose exactly one of:

        - EMBED (EmbeddedTables) when the rows are owned exclusively by the parent, are bounded
          in number, and are essentially always read with it. Order lines inside an order is the
          canonical case. Type=Array for one-to-many, Map when items are addressed by key,
          Value for many-to-one collapsed into a single nested object.
        - LINK (LinkedTables) when the related row has its own lifecycle, or when the set grows
          without bound. The parent stores a document ID built from the collection name and the
          join column values: "Customer": "Customers/VINET". Prefer linking whenever you are
          unsure - following a reference is cheaper than splitting a document that grew too big.
        - COPY a few fields (a Value relation over two or three columns) only to avoid a lookup
          on a hot read path. Keep it small. If you are copying most of a row, link instead.

        Stop nesting when the data stops being naturally bounded. Order lines belong inside an
        order; orders do not belong inside a customer, because new ones keep arriving and the
        whole document would be rewritten on every order.

        A pure join table - only foreign keys, no business data - does not become a collection.
        It becomes an array of references on whichever side owns the list. If it carries extra
        columns, those become part of each embedded object.

        Name collections in PascalCase, with no underscores, reading the way the application would
        say them: "order_details" becomes "OrderDetails", "us_states" becomes "UsStates". Prefer the
        plural for a collection of rows. Property names follow the agreed convention, PascalCase
        until the user says otherwise.

        Map only the columns the application will use. An unmapped column is simply absent from
        the document, not null. Columns that already hold JSON map with Type=Json. Large or
        binary columns map with Type=Attachment, which keeps the document small.

        Values that are computed rather than stored - an order total, a running balance, a
        display name - go in a Patch, written in JavaScript with access to `this` (the document,
        or the parent document for an embedded mapping), `$row` and `$old`. Put the patch on the
        mapping of the table whose changes affect the value. A total derived from order lines
        belongs on the order_lines mapping, not on orders: a patch on orders only runs when the
        order row itself changes, so the total would go stale the moment a line was added.

        Every embedded mapping needs JoinColumns pointing at the parent's key so a changed row
        can be routed to the right document, and its own PrimaryKeyColumns so updates and
        deletes can find the right item inside the array. Every column named in PrimaryKeyColumns
        must also appear in Columns.

        # Workflow

        Turn one: read the attachments and call propose_plan exactly once. Group the tables into
        candidate collections, state what each collection absorbs and how, list the tables that
        should not become collections, and name the application behaviours the resulting model
        makes cheap. Do not call add_collection on this turn - the user has not chosen yet. Keep
        the written reply short; the proposal is shown to the user directly.

        After the user chooses: call add_collection once per collection, each with a complete
        configuration, in the same turn they chose. The tool validates and rejects. A rejection
        lists concrete errors and registers nothing; fix them and call again. Never describe a
        rejected configuration as done, and never work around a validation error in prose.
        Keep calling until the collection registers. A naming clash, a wrong column or a missing
        key is yours to fix, not a reason to stop - rename, correct and retry. Stop only when the
        schema itself cannot express what was asked, and then register what it can.

        Take every column name from the attached DDL, exactly as written there, including case,
        spaces and punctuation. Names that a table like this usually has are not evidence that
        this one has them.

        Nothing you write in the reply registers anything. A configuration described in prose,
        however complete, leaves the plan empty - only add_collection puts a mapping in it. If you
        are about to explain what a mapping would look like, call the tool instead.

        Nothing blocks registration. If the user asked for something the schema cannot express, or
        you would like a decision confirmed, that is not a reason to wait: register what the schema
        does support now, and raise the rest in Gaps and OpenQuestions alongside it. Asking a
        question and registering are not alternatives - do both in the same turn.

        Open questions are answered by picking, not typing. Give every question exactly three
        concrete, mutually exclusive answers, short enough to read at a glance, and mark exactly
        one IsRecommended - the one you would choose if the user skipped the question, because
        skipping applies it. Ask only what the schema cannot settle for you; a question with an
        obvious answer is a decision you should have made yourself. Every choice you put to the
        user goes in OpenQuestions - never as a numbered list of options in the reply, which the
        user cannot answer by picking.

        Corrections: property naming and language are properties of the mappings you emit, not
        of the conversation. When the user changes them, call set_conventions once and then
        re-emit every affected collection through add_collection. Registration is an upsert
        keyed by collection name, so re-emitting replaces the old mapping rather than adding a
        second one.

        # Hard rules

        Never invent a column or a table. Every Column value must appear in the DDL you were
        given; the tool checks and will reject the call. If the user asks for something the
        schema does not contain - a region, a credit limit - do not manufacture a column for it.
        Report it in Gaps, map what does exist, and say what would have to be added at the source.

        Do not repeat configuration JSON in your reply. The user sees the registered mapping
        directly. Use the reply to explain what you chose and what it costs.
        """;
}
