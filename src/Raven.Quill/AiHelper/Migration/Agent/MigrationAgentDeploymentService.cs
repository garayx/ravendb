using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Raven.Quill.Hosting;
using Raven.Quill.Logging;
using Raven.Server.Logging;

namespace Raven.Quill.AiHelper.Migration.Agent;

/// <summary>
/// Puts the schema migration planner in the database once, at startup, so a request never has to
/// check whether the agent it is about to use exists.
///
/// The agent itself is always registered. Its AI connection string is not: that carries a secret,
/// and in a real appliance the operator creates it through /api/ai/connection-strings. This only
/// seeds one when a key is sitting in the environment and nothing has claimed the name yet, which
/// is what makes a local checkout usable without a manual step.
/// </summary>
public sealed class MigrationAgentDeploymentService(
    IDocumentStore store,
    IServerReady ready,
    QuillLogger<MigrationAgentDeploymentService> logger) : BackgroundService
{
    private static readonly TimeSpan ReadinessPollInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The appliance's own key first; the shared integration-test key is a local-checkout
    /// convenience so a developer does not have to set a second variable to see this work.
    /// </summary>
    private static readonly string[] ApiKeyVariables =
    [
        "RAVEN_QUILL_OPENAI_API_KEY",
        "RAVEN_AI_INTEGRATION_OPENAI_API_KEY"
    ];

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (ready.IsReady == false)
                await Task.Delay(ReadinessPollInterval, stoppingToken);

            await EnsureConnectionStringAsync(stoppingToken);

            await SchemaMigrationAgentDefinition.CreateOrUpdateAsync(store, stoppingToken);

            if (logger.IsInfoEnabled)
                logger.Info($"Registered the schema migration planner '{SchemaMigrationAgentDefinition.Identifier}'.");
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        catch (Exception e)
        {
            // A missing key or an unreachable cluster must not stop the appliance booting. The
            // migration endpoints fail loudly on their own if the agent turned out not to be there.
            if (logger.IsWarnEnabled)
                logger.Warn(e, "Could not register the schema migration planner.");
        }
    }

    private async Task EnsureConnectionStringAsync(CancellationToken token)
    {
        var existing = await store.Maintenance.Server.SendAsync(
            new GetServerWideConnectionStringsOperation(
                SchemaMigrationAgentDefinition.ConnectionStringName, ConnectionStringType.Ai), token);

        if (existing is { Results.Count: > 0 })
            return;

        var apiKey = ApiKeyVariables
            .Select(Environment.GetEnvironmentVariable)
            .FirstOrDefault(key => string.IsNullOrWhiteSpace(key) == false);

        if (apiKey is null)
        {
            if (logger.IsInfoEnabled)
            {
                logger.Info(
                    $"No AI connection string named '{SchemaMigrationAgentDefinition.ConnectionStringName}' and no " +
                    $"API key in the environment; create one at /api/ai/connection-strings before using the planner.");
            }

            return;
        }

        await store.Maintenance.Server.SendAsync(
            new PutServerWideConnectionStringOperation(new ServerWideConnectionString
            {
                ConnectionString = new AiConnectionString
                {
                    Name = SchemaMigrationAgentDefinition.ConnectionStringName,
                    ModelType = AiModelType.Chat,
                    OpenAiSettings = new OpenAiSettings(
                        apiKey: apiKey,
                        endpoint: null,
                        model: SchemaMigrationAgentDefinition.Model)
                }
            }), token);

        if (logger.IsInfoEnabled)
            logger.Info($"Seeded AI connection string '{SchemaMigrationAgentDefinition.ConnectionStringName}' from the environment.");
    }
}
