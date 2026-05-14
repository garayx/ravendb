using Raven.AiAppliance.Hosting;
using Xunit;

namespace AiApplianceTests;

public class ServerReadyFlagTests
{
    [Fact]
    public void Starts_not_ready()
    {
        IServerReady flag = new ServerReadyFlag();
        Assert.False(flag.IsReady);
        Assert.Null(flag.LastError);
    }

    [Fact]
    public void MarkReady_flips_state_and_clears_error()
    {
        IServerReady flag = new ServerReadyFlag();
        flag.MarkFailed("boom");
        flag.MarkReady();
        Assert.True(flag.IsReady);
        Assert.Null(flag.LastError);
    }

    [Fact]
    public void MarkFailed_records_error_and_resets_ready()
    {
        IServerReady flag = new ServerReadyFlag();
        flag.MarkReady();
        flag.MarkFailed("connection refused");
        Assert.False(flag.IsReady);
        Assert.Equal("connection refused", flag.LastError);
    }
}
