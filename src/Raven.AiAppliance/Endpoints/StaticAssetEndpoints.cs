using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.FileProviders;

namespace Raven.AiAppliance.Endpoints;

public static class StaticAssetEndpoints
{
    /// Serves the static demo chat UI from wwwroot/. ASP.NET Core's .NET 10
    /// MapStaticAssets pipeline ships *fingerprinted* assets (chat.7wqpx38jhl.css),
    /// which is wrong for plain HTML that references "/chat.css" literally — so
    /// we explicitly construct a PhysicalFileProvider pointed at the publish
    /// output's wwwroot/ and pin the classic UseDefaultFiles + UseStaticFiles
    /// middleware against it. Once T-2's Next.js export takes over, the same
    /// pattern still works.
    public static void Map(WebApplication app)
    {
        var webRoot = Path.Combine(AppContext.BaseDirectory, "wwwroot");
        if (!Directory.Exists(webRoot))
            return;

        var fileProvider = new PhysicalFileProvider(webRoot);

        app.UseDefaultFiles(new DefaultFilesOptions
        {
            FileProvider = fileProvider,
            DefaultFileNames = new List<string> { "index.html" },
        });

        app.UseStaticFiles(new StaticFileOptions
        {
            FileProvider = fileProvider,
        });
    }
}
