// File: tests/MsmqCache.Tests/TcpServerFixture.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Starts the real server host on an ephemeral loopback port for TCP integration tests
// License: MIT

using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using MsmqCache;
using Xunit;

public abstract class TcpServerFixtureBase : IAsyncLifetime
{
    string _dir = "";
    CacheServerService? _svc;

    protected abstract string Password { get; }

    public int Port { get; private set; }
    public string DataDir => Path.Combine(_dir, "data");
    public ConfigManager Config { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _dir = Path.Combine(Path.GetTempPath(), "mcached-it-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        var cfgPath = Path.Combine(_dir, "config.json");
        File.WriteAllText(cfgPath, JsonSerializer.Serialize(new
        {
            EnableTcp = true,
            EnableMsmq = false,
            BindAddress = "127.0.0.1",
            Port = 0,
            Password = Password,
            DataDirectory = DataDir,
            PersistenceEnabled = true,
            SnapshotIntervalSeconds = 3600,
            MaxValueLength = 1000
        }));

        Config = ConfigManager.Load(new[] { "--config", cfgPath });
        var store = new Store(Config.Current);
        var stats = new ServerStats();
        var handler = new CommandHandler(store, Config, stats, NullLogger<CommandHandler>.Instance);
        _svc = new CacheServerService(NullLogger<CacheServerService>.Instance, Config, store, handler, stats);
        await _svc.StartAsync(CancellationToken.None);

        for (int i = 0; i < 100 && _svc.TcpPort == 0; i++) await Task.Delay(50);
        Port = _svc.TcpPort;
        if (Port == 0) throw new InvalidOperationException("server did not start listening");
    }

    public async Task DisposeAsync()
    {
        if (_svc != null) await _svc.StopAsync(CancellationToken.None);
        try { Directory.Delete(_dir, true); } catch { }
    }
}

public sealed class OpenServerFixture : TcpServerFixtureBase
{
    protected override string Password => "";
}

public sealed class SecuredServerFixture : TcpServerFixtureBase
{
    protected override string Password => "s3cret-pw";
}
