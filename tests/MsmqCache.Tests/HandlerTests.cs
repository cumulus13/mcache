// File: tests/MsmqCache.Tests/HandlerTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Command handler: roles, PXAT/EXAT/PEXPIREAT, PUBLISH, hub fan-out (no sockets, no MSMQ)
// License: MIT

using MsmqCache;
using Xunit;

public class HandlerTests
{
    static Response Run(TestHost h, params string[] cmd) =>
        h.Handler.Execute(new Request { Cmd = cmd[0], Args = cmd.Skip(1).ToArray() });

    [Fact]
    public void Replica_Rejects_Writes_Allows_Reads_And_Publish()
    {
        var h = TestHost.InMemory(new ServerConfig { Role = "replica", MaxMemoryMb = 0 });
        var w = Run(h, "SET", "k", "v");
        Assert.Equal(Kinds.Err, w.Kind);
        Assert.StartsWith("READONLY", w.Str);
        Assert.Equal(Kinds.Err, Run(h, "DEL", "k").Kind);
        Assert.Equal(Kinds.Err, Run(h, "FLUSHALL").Kind);
        Assert.Equal(Kinds.Nil, Run(h, "GET", "k").Kind);
        Assert.Equal(Kinds.Num, Run(h, "PUBLISH", "ch", "m").Kind);
    }

    [Fact]
    public void Set_Pxat_Future_And_Past()
    {
        var h = TestHost.InMemory(new ServerConfig { MaxMemoryMb = 0 });
        long future = Store.Now() + 60_000;
        Assert.Equal(Kinds.Ok, Run(h, "SET", "k", "v", "PXAT", future.ToString()).Kind);
        Assert.InRange(Run(h, "PTTL", "k").Num, 50_000, 60_000);

        Assert.Equal(Kinds.Ok, Run(h, "SET", "k", "v2", "PXAT", (Store.Now() - 5).ToString()).Kind);
        Assert.Equal(Kinds.Nil, Run(h, "GET", "k").Kind);                 // past deadline => key deleted

        long exat = (Store.Now() + 90_000) / 1000;
        Assert.Equal(Kinds.Ok, Run(h, "SET", "e", "v", "EXAT", exat.ToString()).Kind);
        Assert.InRange(Run(h, "TTL", "e").Num, 80, 91);
    }

    [Fact]
    public void Pexpireat_Expireat_Persist()
    {
        var h = TestHost.InMemory(new ServerConfig { MaxMemoryMb = 0 });
        Run(h, "SET", "k", "v");
        Assert.Equal(1, Run(h, "PEXPIREAT", "k", (Store.Now() + 30_000).ToString()).Num);
        Assert.InRange(Run(h, "TTL", "k").Num, 25, 31);
        Assert.Equal(1, Run(h, "PERSIST", "k").Num);
        Assert.Equal(-1, Run(h, "TTL", "k").Num);
        Assert.Equal(1, Run(h, "EXPIREAT", "k", ((Store.Now() / 1000) + 100).ToString()).Num);
        Assert.Equal(0, Run(h, "PEXPIREAT", "missing", "1").Num);
    }

    [Fact]
    public void Publish_Counts_LocalSubscribers_And_Delivers_Frame()
    {
        var h = TestHost.InMemory(new ServerConfig());
        Assert.Equal(0, Run(h, "PUBLISH", "news", "hi").Num);

        var sub = h.Hub.CreateSubscriber();
        h.Hub.Subscribe(sub, "news");
        Assert.Equal(1, Run(h, "PUBLISH", "news", "hi").Num);
        Assert.True(sub.Out.Reader.TryRead(out var frame));
        Assert.Equal("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n", System.Text.Encoding.UTF8.GetString(frame));

        h.Hub.Unsubscribe(sub, "news");
        Assert.Equal(0, Run(h, "PUBLISH", "news", "again").Num);
    }

    [Fact]
    public void Publish_Disabled_IsError()
    {
        var h = TestHost.InMemory(new ServerConfig { EnablePubSub = false });
        Assert.Equal(Kinds.Err, Run(h, "PUBLISH", "a", "b").Kind);
    }

    [Fact]
    public void SlowSubscriber_IsFlaggedForDisconnect()
    {
        var hub = new PubSubHub();
        var sub = hub.CreateSubscriber();
        int overflow = 0;
        sub.OnOverflow = () => Interlocked.Increment(ref overflow);
        hub.Subscribe(sub, "c");
        for (int i = 0; i < 1100; i++) hub.Publish("c", "m");   // buffer holds 1000
        Assert.True(overflow > 0);
    }

    [Fact]
    public async Task ExecuteAsync_WithoutWal_BehavesLikeExecute()
    {
        var h = TestHost.InMemory(new ServerConfig { WalMode = "sync" });
        var r = await h.Handler.ExecuteAsync(new Request { Cmd = "SET", Args = new[] { "a", "b" } }, CancellationToken.None);
        Assert.Equal(Kinds.Ok, r.Kind);
    }

    [Fact]
    public void Info_Reports_Roles_Wal_And_PubSub_Sections()
    {
        var h = TestHost.InMemory(new ServerConfig());
        var info = Run(h, "INFO").Str!;
        Assert.Contains("role:master", info);
        Assert.Contains("wal_enabled:0", info);
        Assert.Contains("pubsub_enabled:1", info);
        Assert.Contains("write_seq:", info);
    }

    [Fact]
    public void Config_Validation_Rejects_Bad_Combinations()
    {
        string Tmp(object o)
        {
            var d = Path.Combine(Path.GetTempPath(), "msmqcache-cfg-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(d);
            var p = Path.Combine(d, "config.json");
            File.WriteAllText(p, System.Text.Json.JsonSerializer.Serialize(o));
            return p;
        }
        Assert.Throws<InvalidDataException>(() => ConfigManager.Load(new[] { "--config", Tmp(new { Role = "boss" }) }));
        Assert.Throws<InvalidDataException>(() => ConfigManager.Load(new[] { "--config", Tmp(new { WalMode = "maybe" }) }));
        Assert.Throws<InvalidDataException>(() => ConfigManager.Load(new[] { "--config", Tmp(new { EnableTcp = false, EnableMsmq = false }) }));
        // WAL / replication need MSMQ, i.e. Windows (on Windows these fail for the other reasons below)
        Assert.Throws<InvalidDataException>(() => ConfigManager.Load(new[] { "--config", Tmp(new { WalEnabled = true, PersistenceEnabled = false }) }));
        Assert.Throws<InvalidDataException>(() => ConfigManager.Load(new[] { "--config", Tmp(new { WalEnabled = true, MaxValueLength = 900000 }) }));
    }
}
