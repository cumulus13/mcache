// File: tests/MsmqCache.Tests/WriteLogTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Store sequence/effect logging, replay and replicated-apply semantics (no MSMQ needed)
// License: MIT

using MsmqCache;
using Xunit;

public class WriteLogTests
{
    sealed class CaptureSink : IWriteSink
    {
        public readonly List<(LogEntry E, bool Replicate)> Items = new();
        public void Append(LogEntry entry, bool replicate) => Items.Add((entry, replicate));
    }

    static (Store S, CaptureSink Sink) New(Action<ServerConfig>? tweak = null)
    {
        var c = new ServerConfig { MaxMemoryMb = 0 };
        tweak?.Invoke(c);
        var s = new Store(c);
        var sink = new CaptureSink();
        s.SetSink(sink);
        return (s, sink);
    }

    [Fact]
    public void Set_EmitsSetEntry_WithIncreasingSeq()
    {
        var (s, sink) = New();
        s.Set("a", "1", 0, false, false);
        s.Set("b", "2", 0, false, false);
        Assert.Equal(2, sink.Items.Count);
        Assert.Equal(1, sink.Items[0].E.Seq);
        Assert.Equal(2, sink.Items[1].E.Seq);
        Assert.Equal("SET", sink.Items[0].E.Cmd);
        Assert.Equal(new[] { "a", "1" }, sink.Items[0].E.Args);
        Assert.True(sink.Items[0].Replicate);
        Assert.Equal(2, s.Seq);
    }

    [Fact]
    public void SetWithTtl_LogsAbsoluteDeadline_NotRelative()
    {
        var (s, sink) = New();
        long before = Store.Now();
        s.Set("k", "v", 60_000, false, false);
        var a = sink.Items[0].E.Args;
        Assert.Equal(4, a.Length);
        Assert.Equal("PXAT", a[2]);
        Assert.InRange(long.Parse(a[3]), before + 59_000, Store.Now() + 61_000);
    }

    [Fact]
    public void Del_Expire_Persist_Flush_AreLogged_AsNormalizedEffects()
    {
        var (s, sink) = New();
        s.Set("k", "v", 0, false, false);
        Assert.True(s.Expire("k", 100));
        Assert.True(s.Persist("k"));
        Assert.Equal(1, s.Del(new[] { "k" }));
        s.FlushAll();
        Assert.Equal(new[] { "SET", "PEXPIREAT", "PERSIST", "DEL", "FLUSHALL" }, sink.Items.Select(i => i.E.Cmd).ToArray());
        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, sink.Items.Select(i => i.E.Seq).ToArray());
    }

    [Fact]
    public void NoOps_DoNotEmit_OrAdvanceSequence()
    {
        var (s, sink) = New();
        Assert.Equal(0, s.Del(new[] { "missing" }));
        Assert.False(s.Expire("missing", 10));
        Assert.False(s.Persist("missing"));
        Assert.Empty(sink.Items);
        Assert.Equal(0, s.Seq);
    }

    [Fact]
    public void Incr_Append_LogFullValue()
    {
        var (s, sink) = New();
        s.Incr("n", 5);
        s.Incr("n", 2);
        s.Append("t", "ab");
        Assert.Equal(new[] { "n", "5" }, sink.Items[0].E.Args);
        Assert.Equal(new[] { "n", "7" }, sink.Items[1].E.Args);
        Assert.Equal(new[] { "t", "ab" }, sink.Items[2].E.Args);
    }

    [Fact]
    public void Eviction_And_Expiry_AreLocalAndNotLogged()
    {
        var (s, sink) = New(c => c.MaxKeys = 1);
        s.Set("a", "1", 0, false, false);
        s.Set("b", "2", 0, false, false);          // evicts a
        s.Set("t", "3", 20, false, false);         // evicts b
        Thread.Sleep(80);
        s.Sweep();                                  // expires t
        Assert.Equal(new[] { "SET", "SET", "SET" }, sink.Items.Select(i => i.E.Cmd).ToArray());
    }

    [Fact]
    public void ApplyReplicated_Applied_Duplicate_Gap()
    {
        var (replica, sink) = New();
        LogEntry E(long seq, string cmd, params string[] a) => new() { Seq = seq, Cmd = cmd, Args = a };

        Assert.Equal(ApplyResult.Gap, replica.ApplyReplicated(E(2, "SET", "b", "2")));   // missed seq 1
        Assert.Equal(ApplyResult.Applied, replica.ApplyReplicated(E(1, "SET", "a", "1")));
        Assert.Equal(ApplyResult.Duplicate, replica.ApplyReplicated(E(1, "SET", "a", "1")));
        Assert.Equal(ApplyResult.Applied, replica.ApplyReplicated(E(2, "SET", "b", "2")));

        Assert.Equal("1", replica.Get("a"));
        Assert.Equal("2", replica.Get("b"));
        Assert.Equal(2, replica.Seq);
        // the replica's own WAL gets the primary's sequence numbers and is NOT re-replicated
        Assert.Equal(new long[] { 1, 2 }, sink.Items.Select(i => i.E.Seq).ToArray());
        Assert.All(sink.Items, i => Assert.False(i.Replicate));
    }

    [Fact]
    public void Primary_To_Replica_EndToEnd_ByEntries()
    {
        var (primary, psink) = New();
        var (replica, _) = New();
        primary.Set("a", "1", 0, false, false);
        primary.Set("t", "x", 60_000, false, false);
        primary.Expire("a", 100);
        primary.Del(new[] { "t" });
        primary.Set("c", "3", 0, false, false);
        foreach (var (e, _) in psink.Items) Assert.Equal(ApplyResult.Applied, replica.ApplyReplicated(e));
        Assert.Equal(primary.Seq, replica.Seq);
        Assert.Equal("1", replica.Get("a"));
        Assert.InRange(replica.Ttl("a"), 1, 100);
        Assert.Null(replica.Get("t"));
        Assert.Equal("3", replica.Get("c"));
    }

    [Fact]
    public void Replay_AppliesWithoutEmitting_AndIsIdempotent()
    {
        var (s, sink) = New();
        var e1 = new LogEntry { Seq = 1, Cmd = "SET", Args = new[] { "a", "1" } };
        Assert.True(s.ReplayEntry(e1));
        Assert.False(s.ReplayEntry(e1));                    // already applied
        Assert.Equal("1", s.Get("a"));
        Assert.Equal(1, s.Seq);
        Assert.Empty(sink.Items);
        s.Set("b", "2", 0, false, false);                   // continues numbering after the replayed log
        Assert.Equal(2, sink.Items[0].E.Seq);
    }

    [Fact]
    public void Replay_ExpiredPxat_DeletesInsteadOfSets()
    {
        var (s, _) = New();
        s.ReplayEntry(new LogEntry { Seq = 1, Cmd = "SET", Args = new[] { "k", "v" } });
        s.ReplayEntry(new LogEntry { Seq = 2, Cmd = "SET", Args = new[] { "k", "v2", "PXAT", (Store.Now() - 1000).ToString() } });
        Assert.Null(s.Get("k"));
    }

    [Fact]
    public void Replay_UnknownEntry_Throws()
    {
        var (s, _) = New();
        Assert.Throws<InvalidDataException>(() => s.ReplayEntry(new LogEntry { Seq = 1, Cmd = "NOPE", Args = Array.Empty<string>() }));
    }

    [Fact]
    public void Snapshot_V2_RoundTrips_Sequence_And_Segment()
    {
        var dir = Path.Combine(Path.GetTempPath(), "msmqcache-snap-" + Guid.NewGuid().ToString("N"));
        var path = Path.Combine(dir, "s.json");
        try
        {
            var (s1, _) = New();
            s1.Set("a", "1", 0, false, false);
            s1.Set("b", "2", 60_000, false, false);
            Assert.True(s1.Snapshot(path, force: true, walSegment: 7));
            Assert.Equal(0, s1.SeqSinceSnapshot);

            var (s2, _) = New();
            Assert.Equal(2, s2.Load(path));
            Assert.Equal(2, s2.Seq);
            Assert.Equal(7, s2.LoadedWalSegment);
            Assert.Equal("2", s2.Get("b"));
        }
        finally { if (Directory.Exists(dir)) Directory.Delete(dir, true); }
    }

    [Fact]
    public void ExportInstallSnapshot_ReplicatesFullState_AndRaisesEvent()
    {
        var (primary, _) = New();
        primary.Set("a", "1", 0, false, false);
        primary.Set("b", "2", 60_000, false, false);
        var (seq, entries) = primary.ExportSnapshot();

        var (replica, _) = New();
        replica.Set("stale", "x", 0, false, false);
        bool raised = false;
        replica.SnapshotInstalled += () => raised = true;
        replica.InstallSnapshot(seq, entries);

        Assert.True(raised);
        Assert.Null(replica.Get("stale"));
        Assert.Equal("1", replica.Get("a"));
        Assert.InRange(replica.Ttl("b"), 1, 60);
        Assert.Equal(seq, replica.Seq);
        // the next live entry from the primary applies cleanly
        Assert.Equal(ApplyResult.Applied, replica.ApplyReplicated(new LogEntry { Seq = seq + 1, Cmd = "SET", Args = new[] { "c", "3" } }));
    }

    [Fact]
    public void ReplMessage_And_WalBatch_Serialize_RoundTrip()
    {
        var rm = new ReplMessage
        {
            Type = "w",
            Entries = new List<LogEntry> { new() { Seq = 5, Cmd = "SET", Args = new[] { "k", "v\u00e9 \u2713" } } }
        };
        var back = Wire.Unpack<ReplMessage>(Wire.Pack(rm))!;
        Assert.Equal("w", back.Type);
        Assert.Equal(5, back.Entries![0].Seq);
        Assert.Equal("v\u00e9 \u2713", back.Entries[0].Args[1]);

        var wb = Wire.Unpack<WalBatch>(Wire.Pack(new WalBatch { Entries = rm.Entries! }))!;
        Assert.Single(wb.Entries);
    }
}
