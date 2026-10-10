// File: tests/MsmqCache.MsmqTests/MsmqTransportTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: MSMQ-backed tests (request transport, WAL, replication); skipped unless MSMQCACHE_MSMQ_TESTS=1 on Windows+MSMQ
// License: MIT

using Experimental.System.Messaging;
using MsmqCache;
using Xunit;

public static class Gate
{
    public static bool On => Environment.GetEnvironmentVariable("MSMQCACHE_MSMQ_TESTS") == "1";
    public const string Why = "Set MSMQCACHE_MSMQ_TESTS=1 on a Windows host with the MSMQ feature installed and running.";

    public static string Q(string tag, string id) => $@".\Private$\msmqcache_t_{tag}_{id}";

    public static async Task<bool> Eventually(Func<bool> cond, int seconds = 20)
    {
        var end = DateTime.UtcNow.AddSeconds(seconds);
        while (DateTime.UtcNow < end) { if (cond()) return true; await Task.Delay(100); }
        return cond();
    }

    public static void DeleteQueues(string prefix)
    {
        try
        {
            foreach (var q in MessageQueue.GetPrivateQueuesByMachine("."))
            {
                using (q)
                    if (q.QueueName.Contains(prefix, StringComparison.OrdinalIgnoreCase))
                        try { MessageQueue.Delete(@".\" + q.QueueName); } catch { }
            }
        }
        catch { }
    }
}

public sealed class MsmqFactAttribute : FactAttribute
{
    public MsmqFactAttribute() { if (!Gate.On) Skip = Gate.Why; }
}

// ------------------------------------------------------------------ request/reply transport
public sealed class MsmqRequestFixture : IAsyncLifetime
{
    public string Id { get; } = Guid.NewGuid().ToString("N")[..8];
    public string Queue => Gate.Q("req", Id);
    public TestHost Host { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        if (!Gate.On) return;
        Host = await TestHost.StartAsync(new
        {
            EnableTcp = false, EnableMsmq = true, RequestQueue = Queue, WorkerThreads = 2, PersistenceEnabled = false
        });
        Assert.True(await Gate.Eventually(() => MessageQueue.Exists(Queue)));
    }

    public async Task DisposeAsync()
    {
        if (Host != null) await Host.DisposeAsync();
        if (Gate.On) Gate.DeleteQueues("msmqcache_t_req_" + Id);
    }
}

public class MsmqTransportTests : IClassFixture<MsmqRequestFixture>
{
    readonly MsmqRequestFixture _fx;
    public MsmqTransportTests(MsmqRequestFixture fx) => _fx = fx;

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
    public void Queue_Gets_Configured_Properties()
    {
        using var q = new MessageQueue(_fx.Queue);
        Assert.False(q.Authenticate);
        Assert.Equal(EncryptionRequired.None, q.EncryptionRequired);
    }
}

// ------------------------------------------------------------------ write-ahead log
public class MsmqWalTests
{
    static object Cfg(string wal, string dir, string mode = "sync", long checkpoint = 100000) => new
    {
        EnableTcp = true, Port = 0, BindAddress = "127.0.0.1",
        PersistenceEnabled = true, DataDirectory = Path.Combine(dir, "data"), SnapshotIntervalSeconds = 3600,
        WalEnabled = true, WalQueue = wal, WalMode = mode, WalCheckpointEntries = checkpoint, MaxValueLength = 1000
    };

    static TcpCacheClient Client(TestHost h) => new(new TcpClientOptions { Port = h.Port });

    [MsmqFact]
    public async Task Data_Survives_Restart_From_The_WAL_Alone()
    {
        string id = Guid.NewGuid().ToString("N")[..8], wal = Gate.Q("wal", id);
        string dir = Path.Combine(Path.GetTempPath(), "msmqcache-wal-" + id);
        try
        {
            var h1 = await TestHost.StartAsync(Cfg(wal, dir), dir);
            using (var c = Client(h1))
            {
                for (int i = 0; i < 50; i++) Assert.True(c.Set("k" + i, "v" + i));
                Assert.Equal(1, c.Del("k0"));
                Assert.True(c.Set("ttl", "x", TimeSpan.FromMinutes(5)));
                Assert.Contains("wal_enabled:1", c.Execute("INFO").Str);
            }
            await h1.StopAsync();

            // lose the snapshot: everything must come back from the MSMQ log
            File.Delete(h1.Config.SnapshotPath);

            await using var h2 = await TestHost.StartAsync(Cfg(wal, dir), dir);
            using var c2 = Client(h2);
            Assert.Null(c2.Get("k0"));
            Assert.Equal("v49", c2.Get("k49"));
            Assert.InRange(c2.Ttl("ttl"), 200, 300);
            Assert.Equal(51, c2.Execute("DBSIZE").Num);
            Assert.True(c2.Set("after", "restart"));    // sequence numbering continues
        }
        finally { Gate.DeleteQueues("msmqcache_t_wal_" + id); try { Directory.Delete(dir, true); } catch { } }
    }

    [MsmqFact]
    public async Task Checkpoint_Rotates_Segment_And_Drops_Old_Ones()
    {
        string id = Guid.NewGuid().ToString("N")[..8], wal = Gate.Q("wal", id);
        string dir = Path.Combine(Path.GetTempPath(), "msmqcache-wal-" + id);
        try
        {
            await using var h = await TestHost.StartAsync(Cfg(wal, dir, "async"), dir);
            using var c = Client(h);
            for (int i = 0; i < 20; i++) c.Set("a" + i, "v");
            Assert.True(MessageQueue.Exists(wal + "_00000001"));

            Assert.Equal(Kinds.Ok, c.Execute("SAVE").Kind);   // rotate -> snapshot -> delete segment 1
            Assert.True(MessageQueue.Exists(wal + "_00000002"));
            Assert.True(await Gate.Eventually(() => !MessageQueue.Exists(wal + "_00000001")));
            Assert.Equal("v", c.Get("a19"));
        }
        finally { Gate.DeleteQueues("msmqcache_t_wal_" + id); try { Directory.Delete(dir, true); } catch { } }
    }

    [MsmqFact]
    public async Task Writes_After_Checkpoint_Are_Replayed_On_Top_Of_The_Snapshot()
    {
        string id = Guid.NewGuid().ToString("N")[..8], wal = Gate.Q("wal", id);
        string dir = Path.Combine(Path.GetTempPath(), "msmqcache-wal-" + id);
        try
        {
            var h1 = await TestHost.StartAsync(Cfg(wal, dir), dir);
            using (var c = Client(h1))
            {
                c.Set("old", "1");
                c.Execute("SAVE");
                c.Set("new", "2");
                c.Del("old");
            }
            await h1.StopAsync();
            File.Delete(h1.Config.SnapshotPath);          // worst case: only segment 2 (post-checkpoint) is left
            await using var h2 = await TestHost.StartAsync(Cfg(wal, dir), dir);
            using var c2 = Client(h2);
            Assert.Equal("2", c2.Get("new"));
        }
        finally { Gate.DeleteQueues("msmqcache_t_wal_" + id); try { Directory.Delete(dir, true); } catch { } }
    }

    [MsmqFact]
    public async Task Size_Triggered_Checkpoint_Runs_Automatically()
    {
        string id = Guid.NewGuid().ToString("N")[..8], wal = Gate.Q("wal", id);
        string dir = Path.Combine(Path.GetTempPath(), "msmqcache-wal-" + id);
        try
        {
            await using var h = await TestHost.StartAsync(Cfg(wal, dir, "async", checkpoint: 1000), dir);
            using var c = Client(h);
            for (int i = 0; i < 1200; i++) c.Set("k" + i, "v");
            Assert.True(await Gate.Eventually(() => MessageQueue.Exists(wal + "_00000002"), 15));
        }
        finally { Gate.DeleteQueues("msmqcache_t_wal_" + id); try { Directory.Delete(dir, true); } catch { } }
    }
}

// ------------------------------------------------------------------ replication (primary + replica in one process, two queues)
public class MsmqReplicationTests
{
    static TcpCacheClient Client(TestHost h) => new(new TcpClientOptions { Port = h.Port });

    [MsmqFact]
    public async Task Replica_FullSync_Live_Stream_ReadOnly_And_PubSub()
    {
        string id = Guid.NewGuid().ToString("N")[..8];
        string replQ = Gate.Q("repl", id), ctlQ = Gate.Q("ctl", id);
        try
        {
            // 1. primary first (creates the control queue); it already holds data => the replica must full-sync
            await using var primary = await TestHost.StartAsync(new
            {
                EnableTcp = true, Port = 0, PersistenceEnabled = false, MaxValueLength = 1000,
                Role = "primary", Replicas = new[] { replQ }, ControlQueue = ctlQ
            });
            using var pc = Client(primary);
            for (int i = 0; i < 25; i++) pc.Set("pre" + i, "v" + i);
            pc.Set("ttl", "x", TimeSpan.FromMinutes(5));

            // 2. fresh replica -> hello -> snapshot
            await using var replica = await TestHost.StartAsync(new
            {
                EnableTcp = true, Port = 0, PersistenceEnabled = false, MaxValueLength = 1000,
                Role = "replica", ReplicationQueue = replQ, PrimaryControlQueue = ctlQ, ReplicaAdvertisedQueue = replQ
            });
            using var rc = Client(replica);
            Assert.True(await Gate.Eventually(() => rc.Get("pre24") == "v24", 30), "full sync did not arrive");
            Assert.Equal("v0", rc.Get("pre0"));
            Assert.InRange(rc.Ttl("ttl"), 200, 300);

            // 3. live stream: set / del / expire / incr
            pc.Set("live", "1");
            pc.Incr("counter"); pc.Incr("counter");
            pc.Del("pre0");
            Assert.True(await Gate.Eventually(() => rc.Get("live") == "1" && rc.Get("counter") == "2" && rc.Get("pre0") == null, 20));
            Assert.Equal(primary.Store.Seq, replica.Store.Seq);

            // 4. replica is read-only
            var w = rc.Execute("SET", "x", "y");
            Assert.Equal(Kinds.Err, w.Kind);
            Assert.StartsWith("READONLY", w.Str);
            Assert.Contains("role:slave", rc.Execute("INFO").Str);

            // 5. PUBLISH on the primary reaches a subscriber on the replica
            var sub = replica.Hub.CreateSubscriber();
            replica.Hub.Subscribe(sub, "events");
            pc.Execute("PUBLISH", "events", "hello-replica");
            var got = await Task.Run(() =>
            {
                var end = DateTime.UtcNow.AddSeconds(20);
                while (DateTime.UtcNow < end) { if (sub.Out.Reader.TryRead(out var f)) return System.Text.Encoding.UTF8.GetString(f); Thread.Sleep(50); }
                return "";
            });
            Assert.Contains("hello-replica", got);
        }
        finally { Gate.DeleteQueues("msmqcache_t_repl_" + id); Gate.DeleteQueues("msmqcache_t_ctl_" + id); }
    }

    [MsmqFact]
    public async Task Replica_Recovers_From_A_Gap_With_A_Full_Resync()
    {
        string id = Guid.NewGuid().ToString("N")[..8];
        string replQ = Gate.Q("repl", id), ctlQ = Gate.Q("ctl", id);
        string rdir = Path.Combine(Path.GetTempPath(), "msmqcache-replica-" + id);
        object ReplicaCfg() => new
        {
            EnableTcp = true, Port = 0, MaxValueLength = 1000,
            PersistenceEnabled = true, DataDirectory = Path.Combine(rdir, "data"), SnapshotIntervalSeconds = 3600,
            Role = "replica", ReplicationQueue = replQ, PrimaryControlQueue = ctlQ, ReplicaAdvertisedQueue = replQ
        };
        try
        {
            await using var primary = await TestHost.StartAsync(new
            {
                EnableTcp = true, Port = 0, PersistenceEnabled = false, MaxValueLength = 1000,
                Replicas = new[] { replQ }, ControlQueue = ctlQ
            });
            using var pc = Client(primary);

            var r1 = await TestHost.StartAsync(ReplicaCfg(), rdir);
            using (var rc = Client(r1))
            {
                pc.Set("a", "1");
                Assert.True(await Gate.Eventually(() => rc.Get("a") == "1"));
            }
            await r1.StopAsync();                       // final snapshot: replica is now at sequence N

            // while the replica is down: two entries are queued for it, then the queue is wiped (simulates lost traffic)
            pc.Set("lost1", "x"); pc.Set("lost2", "y");
            Assert.True(await Gate.Eventually(() => { using var q = new MessageQueue(replQ); return q.GetAllMessages().Length >= 1; }, 15));
            using (var q = new MessageQueue(replQ)) q.Purge();
            pc.Set("after-gap", "z");                   // sequence N+3 while the replica expects N+1

            await using var r2 = await TestHost.StartAsync(ReplicaCfg(), rdir);
            using var rc2 = Client(r2);
            Assert.True(await Gate.Eventually(() => rc2.Get("after-gap") == "z" && rc2.Get("lost2") == "y" && rc2.Get("a") == "1", 40),
                        "replica did not recover from the gap");
        }
        finally
        {
            Gate.DeleteQueues("msmqcache_t_repl_" + id); Gate.DeleteQueues("msmqcache_t_ctl_" + id);
            try { Directory.Delete(rdir, true); } catch { }
        }
    }
}
