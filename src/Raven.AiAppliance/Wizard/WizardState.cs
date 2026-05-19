using Raven.Client.Documents.Operations.CdcSink.Schema;

namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Single-tenant wizard state, persisted at the fixed id <see cref="DocumentId"/>
/// in <c>ai-appliance-config</c>. Each wizard step overwrites its slice; no
/// sessionId, no TTL, no GC (per Ayende RavenDB-26629).
/// </summary>
internal sealed class WizardState
{
    public const string DocumentId = "wizard-state";

    public string? Provider { get; set; }
    public string? ConnectionString { get; set; }

    public ConnectResult? LastVerifyResult { get; set; }
    public DateTime? LastVerifyAt { get; set; }

    // CdcSinkSourceSchema is internal in Raven.Client — accessible here via
    // InternalsVisibleTo("Raven.AiAppliance"). Persisting it couples the
    // wizard-state doc shape to the internal schema-discovery shape; accepted
    // trade-off for in-tree code (forces the enclosing type to be internal too).
    public CdcSinkSourceSchema? LastDiscoveredSchema { get; set; }
    public DateTime? LastDiscoverAt { get; set; }
}
