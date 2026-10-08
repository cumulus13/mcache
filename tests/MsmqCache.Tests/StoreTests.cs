// File: tests/MsmqCache.Tests/StoreTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Unit tests for Store (no MSMQ needed): TTL, LRU, limits, snapshot, glob
// License: MIT

using MsmqCache;
using Xunit;

public class StoreTests
{
    static Store New(Action<ServerConfig>? tweak = null)
    {
        var c = new ServerConfig { MaxMemoryMb = 0 };
        tweak?.Invoke(c);
        return new Store(c);
    }

    [Fact]
    public void SetGet_RoundTrip_AndMiss()
    {
        var s = New();
        Assert.True(s.Set("k", "v", 0, false, false));
        Assert.Equal("v", s.Get("k"));
        Assert.Null(s.Get("missing"));
    }

    [Fact]
    public void Nx_Xx_Semantics()
    {
        var s = New();
        Assert.False(s.Set("k", "v", 0, false, true));   // XX on missing
        Assert.True(s.Set("k", "v", 0, true, false));    // NX on missing
        Assert.False(s.Set("k", "w", 0, true, false));   // NX on existing
        Assert.True(s.Set("k", "w", 0, false, true));    // XX on existing
        Assert.Equal("w", s.Get("k"));
    }

    [Fact]
    public void Ttl_ExpiresLazily()
    {
        var s = New();
        s.Set("k", "v", 50, false, false);
        Assert.InRange(s.Ttl("k"), 1, 1);
        Thread.Sleep(150);
        Assert.Null(s.Get("k"));
        Assert.Equal(-2, s.Ttl("k"));
    }

    [Fact]
    public void Sweep_RemovesExpired()
    {
        var s = New();
        s.Set("a", "1", 30, false, false);
        s.Set("b", "2", 0, false, false);
        Thread.Sleep(100);
        Assert.Equal(1, s.Sweep());
        Assert.Equal(1, s.DbSize());
    }

    [Fact]
    public void Persist_And_Expire()
    {
        var s = New();
        s.Set("k", "v", 60_000, false, false);
        Assert.True(s.Persist("k"));
        Assert.Equal(-1, s.Ttl("k"));
        Assert.True(s.Expire("k", 30));
        Assert.InRange(s.Ttl("k"), 1, 30);
        Assert.True(s.Expire("k", 0));                   // <=0 deletes
        Assert.Equal(-2, s.Ttl("k"));
    }

    [Fact]
    public void Incr_Works_AndRejectsNonInteger()
    {
        var s = New();
        Assert.Equal(1, s.Incr("n", 1));
        Assert.Equal(6, s.Incr("n", 5));
        Assert.Equal(5, s.Incr("n", -1));
        s.Set("t", "abc", 0, false, false);
        Assert.Throws<CacheException>(() => s.Incr("t", 1));
        s.Set("big", long.MaxValue.ToString(), 0, false, false);
        Assert.Throws<CacheException>(() => s.Incr("big", 1));
    }

    [Fact]
    public void Lru_EvictsLeastRecentlyUsed()
    {
        var s = New(c => c.MaxKeys = 2);
        s.Set("a", "1", 0, false, false);
        s.Set("b", "2", 0, false, false);
        s.Get("a");                                      // a is now MRU, b is LRU
        s.Set("c", "3", 0, false, false);                // evicts b
        Assert.Null(s.Get("b"));
        Assert.Equal("1", s.Get("a"));
        Assert.Equal("3", s.Get("c"));
    }

    [Fact]
    public void NoEviction_ThrowsOom_WhenFull()
    {
        var s = New(c => { c.MaxKeys = 1; c.EvictionPolicy = "noeviction"; });
        s.Set("a", "1", 0, false, false);
        Assert.Throws<OomException>(() => s.Set("b", "2", 0, false, false));
        Assert.True(s.Set("a", "9", 0, false, false));   // overwrite is fine
    }

    [Fact]
    public void ItemLargerThanMaxMemory_IsRejected_WithoutWipingCache()
    {
        var s = New(c => c.MaxMemoryMb = 1);
        s.Set("small", "x", 0, false, false);
        var huge = new string('x', 600_000);             // ~1.2MB in the size model
        Assert.Throws<OomException>(() => s.Set("huge", huge, 0, false, false));
        Assert.Equal("x", s.Get("small"));
    }

    [Fact]
    public void DefaultTtl_AppliedWhenNoneGiven()
    {
        var s = New(c => c.DefaultTtlSeconds = 100);
        s.Set("k", "v", 0, false, false);
        Assert.InRange(s.Ttl("k"), 1, 100);
    }

    [Fact]
    public void Keys_Glob()
    {
        var s = New();
        s.Set("user:1", "a", 0, false, false);
        s.Set("user:2", "b", 0, false, false);
        s.Set("user:10", "c", 0, false, false);
        s.Set("other", "d", 0, false, false);
        Assert.Equal(3, s.Keys("user:*").Length);
        Assert.Equal(2, s.Keys("user:?").Length);
        Assert.Equal(4, s.Keys("*").Length);
        Assert.Single(s.Keys("other"));
    }

    [Fact]
    public void Del_Exists_MGet()
    {
        var s = New();
        s.Set("a", "1", 0, false, false);
        s.Set("b", "2", 0, false, false);
        Assert.Equal(2, s.Exists(new[] { "a", "b", "zz" }));
        Assert.Equal(new string?[] { "1", null, "2" }, s.MGet(new[] { "a", "zz", "b" }));
        Assert.Equal(1, s.Del(new[] { "a", "zz" }));
        Assert.Equal(1, s.DbSize());
    }

    [Fact]
    public void Snapshot_RoundTrip_PreservesValuesAndTtl()
    {
        var dir = Path.Combine(Path.GetTempPath(), "msmqcache-test-" + Guid.NewGuid().ToString("N"));
        var path = Path.Combine(dir, "snap.json");
        try
        {
            var s1 = New();
            s1.Set("plain", "v1", 0, false, false);
            s1.Set("ttl", "v2", 60_000, false, false);
            Assert.True(s1.Snapshot(path, force: false));
            Assert.False(s1.Snapshot(path, force: false));   // clean => no rewrite

            var s2 = New();
            Assert.Equal(2, s2.Load(path));
            Assert.Equal("v1", s2.Get("plain"));
            Assert.Equal(-1, s2.Ttl("plain"));
            Assert.Equal("v2", s2.Get("ttl"));
            Assert.InRange(s2.Ttl("ttl"), 1, 60);
            Assert.False(File.Exists(path + ".tmp"));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Load_MissingFile_ReturnsZero()
    {
        Assert.Equal(0, New().Load(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"))));
    }
}
