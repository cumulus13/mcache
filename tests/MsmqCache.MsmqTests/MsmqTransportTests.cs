// File: tests/MsmqCache.MsmqTests/MsmqTransportTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: MSMQ transport tests; skipped unless MSMQCACHE_MSMQ_TESTS=1 on a Windows host with MSMQ
// License: MIT

using System.Text.Json;
using Experimental.System.Messaging;
using Microsoft.Extensions.Logging.Abstractions;
using MsmqCache;
using Xunit;

public sealed class MsmqFactAttribute : FactAttribute
{
    public MsmqFactAttribute()
    {
        if (Environment.GetEnvironmentVariable("MSMQCACHE_MSMQ_TESTS") != "1")
            Skip = "Set MSMQCACHE_MSMQ_TESTS=1 on a Windows host with the MSMQ feature installed and running.";
    }
}

public sealed class MsmqFixture : IAsyncLifetime
{
    string _dir = "";
    CacheServerService? _svc;
    public string Queue { get; } = $@".\Private$\MsmqCacheTest_{Guid.NewGuid():N}";
    public ConfigManager Config { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("MSMQCACHE_MSMQ_TESTS") != "1") return;

        _dir = Path.Combine(Path.GetTempPath(), "mcached-msmq-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
        var cfgPath = Path.Combine(_dir, "config.json");
        File.WriteAllText(cfgPath, JsonSerializer.Serialize(new
        {
            EnableTcp = false,
            EnableMsmq = true,
            RequestQueue = Queue,
            WorkerThreads = 2,
            DataDirectory = Path.Combine(_dir, "data"),
            PersistenceEnabled = false
        }));
        Config = ConfigManager.Load(new[] { "--config", cfgPath });
        var store = new Store(Config.Current);
        var stats = new ServerStats();
        var handler = new CommandHandler(store, Config, stats, NullLogger<CommandHandler>.Instance);
        _svc = new CacheServerService(NullLogger<CacheServerService>.Instance, Config, store, handler, stats);
        await _svc.StartAsync(CancellationToken.None);
        for (int i = 0; i < 100 && !MessageQueue.Exists(Queue); i++) await Task.Delay(100);
    }

    public async Task DisposeAsync()
    {
        if (_svc != null) await _svc.StopAsync(CancellationToken.None);
        try { if (MessageQueue.Exists(Queue)) MessageQueue.Delete(Queue); } catch { }
        try { if (_dir.Length > 0) Directory.Delete(_dir, true); } catch { }
    }
}

public class MsmqTransportTests : IClassFixture<MsmqFixture>
{
    readonly MsmqFixture _fx;
    public MsmqTransportTests(MsmqFixture fx) => _fx = fx;

    [MsmqFact]
    public void Ping_Set_Get_Del_Roundtrip()
    {
        using var c = new MsmqCacheClient(_fx.Queue, TimeSpan.FromSeconds(10));
        Assert.True(c.Ping());
        Assert.True(c.Set("k", "v"));
        Assert.Equal("v", c.Get("k"));
        Assert.Equal(1, c.Del("k"));
        Assert.Null(c.Get("k"));
    }

    [MsmqFact]
    public void Concurrent_Clients_Incr_IsAtomic()
    {
        Parallel.For(0, 4, _ =>
        {
            using var c = new MsmqCacheClient(_fx.Queue, TimeSpan.FromSeconds(10));
            for (int i = 0; i < 25; i++) c.Incr("n");
        });
        using var check = new MsmqCacheClient(_fx.Queue, TimeSpan.FromSeconds(10));
        Assert.Equal("100", check.Get("n"));
    }

    [MsmqFact]
    public void Config_SetGet_PersistsToFile()
    {
        using var c = new MsmqCacheClient(_fx.Queue, TimeSpan.FromSeconds(10));
        Assert.Equal(Kinds.Ok, c.Execute("CONFIG", "SET", "maxkeys", "1234").Kind);
        Assert.Equal(new string?[] { "maxkeys", "1234" }, c.Execute("CONFIG", "GET", "maxkeys").Arr);
        Assert.Contains("1234", File.ReadAllText(_fx.Config.FilePath));
    }

    [MsmqFact]
    public void Queue_Gets_Configured_Properties()
    {
        using var q = new MessageQueue(_fx.Queue);
        Assert.False(q.Authenticate);
        Assert.Equal(EncryptionRequired.None, q.EncryptionRequired);
    }
}
