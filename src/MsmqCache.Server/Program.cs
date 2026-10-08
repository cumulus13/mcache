// File: src/MsmqCache.Server/Program.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Server entry point (console or Windows Service)
// License: MIT

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MsmqCache;

ConfigManager cfg;
try { cfg = ConfigManager.Load(args); }
catch (Exception ex)
{
    Console.Error.WriteLine("Config error: " + ex.Message);
    return 2;
}

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddWindowsService(o => o.ServiceName = "MsmqCache");
builder.Services.AddSingleton(cfg);
builder.Services.AddSingleton(sp => new Store(cfg.Current));
builder.Services.AddSingleton<CommandHandler>();
builder.Services.AddHostedService<CacheServerService>();
builder.Build().Run();
return 0;
