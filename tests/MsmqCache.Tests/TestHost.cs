// File: tests/MsmqCache.Tests/TestHost.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Builds and starts the real server host in-process for tests (temp dir, ephemeral port)
// License: MIT

using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using MsmqCache;

public sealed class TestHost : IAsyncDisposable
{
    readonly bool _ownsDir;
    bool _started, _stopped;

    public string Dir { get; }
    public ConfigManager Config { get; }
    public Store Store { get; }
    public ServerStats Stats { get; } = new();
    public WriteDispatcher Disp { get; } = new();
    public PubSubHub Hub { get; } = new();
    public Checkpointer Ckpt { get; }
    public CommandHandler Handler { get; }
    public CacheServerService Service { get; }
    public int Port => Service.TcpPort;

    TestHost(ConfigManager cfg, string dir, bool ownsDir)
    {
        Dir = dir; _ownsDir = ownsDir; Config = cfg;
        Store = new Store(cfg.Current);
        Ckpt = new Checkpointer(cfg, Store, Disp, NullLogger<Checkpointer>.Instance);
        Handler = new CommandHandler(Store, cfg, Stats, Disp, Hub, Ckpt, NullLogger<CommandHandler>.Instance);
        Service = new CacheServerService(NullLogger<CacheServerService>.Instance, cfg, Store, Handler, Stats, Disp, Hub, Ckpt);
    }

    // No disk, no validation: for handler/store level tests.
    public static TestHost InMemory(ServerConfig cfg) => new(ConfigManager.ForTesting(cfg), Path.GetTempPath(), false);

    // Writes config.json (anonymous object) into a temp dir (or the given one), loads it with validation and starts the service.
    public static async Task<TestHost> StartAsync(object configObject, string? dir = null)
    {
        bool owns = dir == null;
        dir ??= Path.Combine(Path.GetTempPath(), "msmqcache-it-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        var cfgPath = Path.Combine(dir, "config.json");
        File.WriteAllText(cfgPath, JsonSerializer.Serialize(configObject));
        var cm = ConfigManager.Load(new[] { "--config", cfgPath });
        var host = new TestHost(cm, dir, owns);
        await host.Service.StartAsync(CancellationToken.None);
        host._started = true;
        if (cm.Current.EnableTcp)
        {
            for (int i = 0; i < 100 && host.Port == 0; i++) await Task.Delay(50);
            if (host.Port == 0) throw new InvalidOperationException("server did not start listening");
        }
        return host;
    }

    public async Task StopAsync()
    {
        if (_started && !_stopped) { _stopped = true; await Service.StopAsync(CancellationToken.None); }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        if (_ownsDir) { try { Directory.Delete(Dir, true); } catch { } }
    }
}
