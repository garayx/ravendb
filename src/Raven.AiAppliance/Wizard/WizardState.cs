namespace Raven.AiAppliance.Wizard;

/// <summary>
/// Single-tenant wizard state, persisted at the fixed id <see cref="DocumentId"/>
/// in <c>ai-appliance-config</c>. Each wizard step overwrites its slice; no
/// sessionId, no TTL, no GC (per Ayende RavenDB-26629).
/// </summary>
public sealed class WizardState
{
    public const string DocumentId = "wizard-state";

    public string? Provider { get; set; }
    public string? ConnectionString { get; set; }

    public ConnectResult? LastVerifyResult { get; set; }
    public DateTime? LastVerifyAt { get; set; }

    // Re-type to CdcSinkSourceSchema (internal, accessed via InternalsVisibleTo)
    // once feature/cdc is rebased in — that branch carries the new client-side
    // CDC schema-discovery operation. Until then, store the schema as raw object
    // to keep the appliance compiling without the missing type.
    public object? LastDiscoveredSchema { get; set; }
    public DateTime? LastDiscoverAt { get; set; }
}
