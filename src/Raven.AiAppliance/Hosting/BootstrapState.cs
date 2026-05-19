namespace Raven.AiAppliance.Hosting;

/// <summary>
/// Top-level boot mode of the appliance, driven by whether a setup package
/// has been redeemed and applied yet.
/// </summary>
public enum BootstrapPhase
{
    /// <summary>
    /// `/setup/` is empty (or missing the appliance config). RavenDB has not
    /// started; only <c>/api/bootstrap/*</c> + the first-run UI are live.
    /// </summary>
    NeedsActivation,

    /// <summary>
    /// A license-redemption call is in flight — fetching the setup package,
    /// unpacking, initialising the secured `IDocumentStore`. Non-bootstrap
    /// endpoints still return 503.
    /// </summary>
    Redeeming,

    /// <summary>
    /// Setup package installed + RavenDB reachable; wizard endpoints live.
    /// </summary>
    Ready,
}

public interface IBootstrapState
{
    BootstrapPhase Phase { get; }
    string? Reason { get; }

    void MarkRedeeming();
    void MarkReady();
    void MarkFailed(string reason);
}

public sealed class BootstrapStateFlag : IBootstrapState
{
    private int _phase = (int)BootstrapPhase.NeedsActivation;
    private string? _reason;

    public BootstrapPhase Phase => (BootstrapPhase)Volatile.Read(ref _phase);
    public string? Reason => Volatile.Read(ref _reason);

    public void MarkRedeeming()
    {
        Volatile.Write(ref _reason, null);
        Volatile.Write(ref _phase, (int)BootstrapPhase.Redeeming);
    }

    public void MarkReady()
    {
        Volatile.Write(ref _reason, null);
        Volatile.Write(ref _phase, (int)BootstrapPhase.Ready);
    }

    public void MarkFailed(string reason)
    {
        Volatile.Write(ref _reason, reason);
        Volatile.Write(ref _phase, (int)BootstrapPhase.NeedsActivation);
    }
}
