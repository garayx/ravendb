using System.ComponentModel.DataAnnotations;

namespace Raven.AiAppliance.Hosting;

public sealed class ApplianceOptions
{
    public const string SectionName = "Appliance";

    [Required] public string RavenUrl { get; set; } = "http://127.0.0.1:8080";
    [Required] public string WebListenUrl { get; set; } = "http://0.0.0.0:5000";
    [Required] public string ConfigDatabase { get; set; } = ApplianceDatabases.Config;

    public string LlmProvider { get; set; } = "openai";
    public string LlmEndpoint { get; set; } = "https://api.openai.com/v1/";
    public string LlmModel { get; set; } = "gpt-4o-mini";
    public string LlmApiKey { get; set; } = "";
    public string LlmConnectionStringName { get; set; } = "appliance-llm";

    /// <summary>
    /// Silent grace period before the first readiness probe fires. RavenDB
    /// reliably takes ~10-15s to start, so pinging earlier just generates
    /// noise. Logged once at info level, then we wait.
    /// </summary>
    public TimeSpan ReadinessInitialDelay { get; set; } = TimeSpan.FromSeconds(15);

    public TimeSpan ReadinessAttemptTimeout { get; set; } = TimeSpan.FromSeconds(2);
    public TimeSpan ReadinessOverallTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
