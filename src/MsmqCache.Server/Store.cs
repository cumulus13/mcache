// File: src/MsmqCache.Server/Store.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: In-memory store: TTL, exact LRU eviction, memory/key limits, atomic snapshots
// License: MIT

using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace MsmqCache;

public class CacheException : Exception { public CacheException(string m) : base(m) { } }
public sealed class OomException : CacheException
{
    public OomException() : base("OOM command not allowed when used memory > 'maxmemory'") { }
}

public sealed class SnapEntry
{
    public string K { get; set; } = "";
    public string V { get; set; } = "";
    public long E { get; set; }   // absolute unix ms, 0 = none
}

public sealed class Store
{
    const int MaxKeysReturned = 10_000;

    sealed class Entry
    {
        public string Key = "", Value = "";
        public long ExpireAt, Size;
        public LinkedListNode<Entry>? Node;
    }

    sealed class TtlCmp : IComparer<(long At, string Key)>
    {
        public int Compare((long At, string Key) a, (long At, string Key) b)
        {
            int c = a.At.CompareTo(b.At);
            return c != 0 ? c : string.CompareOrdinal(a.Key, b.Key);
        }
    }

    readonly ServerConfig _cfg;
    readonly object _sync = new();       // guards all state (single lock = simple + correct)
    readonly object _snapSync = new();   // serialises snapshot writers
    readonly Dictionary<string, Entry> _map = new(StringComparer.Ordinal);
    readonly LinkedList<Entry> _lru = new();   // First = most recently used
    readonly SortedSet<(long At, string Key)> _ttl = new(new TtlCmp());
    readonly long _startMs = Now();
    long _used, _dirty, _savedDirty, _hits, _misses, _expired, _evicted, _lastSaveMs;
    public long Commands;

    public Store(ServerConfig cfg) => _cfg = cfg;

    public static long Now() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    static long SizeOf(string k, string v) => 2L * (k.Length + v.Length) + 96;

    // ---- internals (call under _sync) ----
    Entry? Find(string key, long now)
    {
        if (!_map.TryGetValue(key, out var e)) return null;
        if (e.ExpireAt != 0 && e.ExpireAt <= now) { Remove(e); _expired++; return null; }
        return e;
    }

    void Touch(Entry e) { _lru.Remove(e.Node!); _lru.AddFirst(e.Node!); }

    void Remove(Entry e)
    {
        _map.Remove(e.Key);
        _lru.Remove(e.Node!);
        if (e.ExpireAt != 0) _ttl.Remove((e.ExpireAt, e.Key));
        _used -= e.Size;
    }

    void SetExpire(Entry e, long at)
    {
        if (e.ExpireAt != 0) _ttl.Remove((e.ExpireAt, e.Key));
        e.ExpireAt = at;
        if (at != 0) _ttl.Add((at, e.Key));
    }

    void MakeRoom(long addBytes, int addKeys, string protect)
    {
        long maxBytes = _cfg.MaxMemoryMb * 1024L * 1024L, maxKeys = _cfg.MaxKeys;
        bool Over() => (maxBytes > 0 && _used + addBytes > maxBytes) || (maxKeys > 0 && _map.Count + addKeys > maxKeys);
        if (!Over()) return;
        if (_cfg.EvictionPolicy != "allkeys-lru") throw new OomException();
        var node = _lru.Last;
        while (Over() && node != null)
        {
            var prev = node.Previous;
            if (node.Value.Key != protect) { Remove(node.Value); _evicted++; }
            node = prev;
        }
        if (Over()) throw new OomException();
    }

    void Put(string key, string value, long expireAt)
    {
        _map.TryGetValue(key, out var e);
        long newSize = SizeOf(key, value);
        long maxBytes = _cfg.MaxMemoryMb * 1024L * 1024L;
        if (maxBytes > 0 && newSize > maxBytes) throw new OomException();   // don't wipe cache for an item that can never fit
        long delta = newSize - (e?.Size ?? 0);
        MakeRoom(delta, e == null ? 1 : 0, key);
        if (e == null)
        {
            e = new Entry { Key = key };
            e.Node = _lru.AddFirst(e);
            _map[key] = e;
        }
        else Touch(e);
        _used += delta;
        e.Value = value;
        e.Size = newSize;
        SetExpire(e, expireAt);
        _dirty++;
    }

    // ---- public API ----
    public string? Get(string key)
    {
        lock (_sync)
        {
            var e = Find(key, Now());
            if (e == null) { _misses++; return null; }
            _hits++; Touch(e); return e.Value;
        }
    }

    public string?[] MGet(string[] keys)
    {
        lock (_sync)
        {
            long now = Now();
            var r = new string?[keys.Length];
            for (int i = 0; i < keys.Length; i++)
            {
                var e = Find(keys[i], now);
                if (e == null) { _misses++; continue; }
                _hits++; Touch(e); r[i] = e.Value;
            }
            return r;
        }
    }

    public bool Set(string key, string value, long ttlMs, bool nx, bool xx)
    {
        lock (_sync)
        {
            long now = Now();
            var e = Find(key, now);
            if (nx && e != null) return false;
            if (xx && e == null) return false;
            if (ttlMs <= 0 && _cfg.DefaultTtlSeconds > 0) ttlMs = _cfg.DefaultTtlSeconds * 1000;
            Put(key, value, ttlMs > 0 ? now + Math.Min(ttlMs, MaxTtlMs) : 0);
            return true;
        }
    }

    public int Del(string[] keys)
    {
        lock (_sync)
        {
            long now = Now(); int n = 0;
            foreach (var k in keys)
            {
                var e = Find(k, now);
                if (e != null) { Remove(e); n++; _dirty++; }
            }
            return n;
        }
    }

    public int Exists(string[] keys)
    {
        lock (_sync)
        {
            long now = Now(); int n = 0;
            foreach (var k in keys) if (Find(k, now) != null) n++;
            return n;
        }
    }

    public long Incr(string key, long delta)
    {
        lock (_sync)
        {
            long now = Now();
            var e = Find(key, now);
            long cur = 0;
            if (e != null && !long.TryParse(e.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out cur))
                throw new CacheException("value is not an integer or out of range");
            long next;
            try { next = checked(cur + delta); }
            catch (OverflowException) { throw new CacheException("increment or decrement would overflow"); }
            long exp = e != null ? e.ExpireAt : (_cfg.DefaultTtlSeconds > 0 ? now + _cfg.DefaultTtlSeconds * 1000 : 0);
            Put(key, next.ToString(CultureInfo.InvariantCulture), exp);
            return next;
        }
    }

    const long MaxTtlMs = 100L * 365 * 24 * 3600 * 1000;   // clamp: avoids now+ttl overflow

    public bool Expire(string key, long seconds) => ExpireMs(key, seconds >= MaxTtlMs / 1000 ? MaxTtlMs : seconds * 1000);

    public bool ExpireMs(string key, long ms)
    {
        lock (_sync)
        {
            long now = Now();
            var e = Find(key, now);
            if (e == null) return false;
            if (ms <= 0) Remove(e);
            else SetExpire(e, now + Math.Min(ms, MaxTtlMs));
            _dirty++;
            return true;
        }
    }

    public long PTtl(string key)
    {
        lock (_sync)
        {
            long now = Now();
            var e = Find(key, now);
            if (e == null) return -2;
            if (e.ExpireAt == 0) return -1;
            return Math.Max(0, e.ExpireAt - now);
        }
    }

    public string? GetDel(string key)
    {
        lock (_sync)
        {
            var e = Find(key, Now());
            if (e == null) { _misses++; return null; }
            _hits++;
            var v = e.Value;
            Remove(e); _dirty++;
            return v;
        }
    }

    // Returns the new length in UTF-8 bytes (Redis semantics).
    public long Append(string key, string suffix)
    {
        lock (_sync)
        {
            long now = Now();
            var e = Find(key, now);
            string cur = e?.Value ?? "";
            if ((long)cur.Length + suffix.Length > _cfg.MaxValueLength) throw new CacheException("value too large");
            string next = cur + suffix;
            long exp = e != null ? e.ExpireAt : (_cfg.DefaultTtlSeconds > 0 ? now + _cfg.DefaultTtlSeconds * 1000 : 0);
            Put(key, next, exp);
            return Encoding.UTF8.GetByteCount(next);
        }
    }

    public long Ttl(string key)
    {
        lock (_sync)
        {
            long now = Now();
            var e = Find(key, now);
            if (e == null) return -2;
            if (e.ExpireAt == 0) return -1;
            return (e.ExpireAt - now + 999) / 1000;
        }
    }

    public bool Persist(string key)
    {
        lock (_sync)
        {
            var e = Find(key, Now());
            if (e == null || e.ExpireAt == 0) return false;
            SetExpire(e, 0); _dirty++;
            return true;
        }
    }

    public string[] Keys(string pattern)
    {
        Regex? rx = pattern == "*" ? null
            : new Regex("^" + Regex.Escape(pattern).Replace("\\*", ".*").Replace("\\?", ".") + "$",
                        RegexOptions.Singleline | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        lock (_sync)
        {
            long now = Now();
            var res = new List<string>();
            foreach (var e in _map.Values)
            {
                if (e.ExpireAt != 0 && e.ExpireAt <= now) continue;
                if (rx == null || rx.IsMatch(e.Key)) { res.Add(e.Key); if (res.Count >= MaxKeysReturned) break; }
            }
            return res.ToArray();
        }
    }

    public int DbSize() { lock (_sync) return _map.Count; }

    public void FlushAll()
    {
        lock (_sync) { _map.Clear(); _lru.Clear(); _ttl.Clear(); _used = 0; _dirty++; }
    }

    public int Sweep(int max = 5000)
    {
        lock (_sync)
        {
            long now = Now(); int n = 0;
            while (n < max && _ttl.Count > 0)
            {
                var m = _ttl.Min;
                if (m.At > now) break;
                if (_map.TryGetValue(m.Key, out var e)) { Remove(e); _expired++; _dirty++; }
                else _ttl.Remove(m);
                n++;
            }
            return n;
        }
    }

    // ---- persistence: atomic (tmp + fsync + move) ----
    public bool Snapshot(string path, bool force)
    {
        lock (_snapSync)
        {
            List<SnapEntry> list; long mark;
            lock (_sync)
            {
                if (!force && _dirty == _savedDirty) return false;
                long now = Now();
                list = new(_map.Count);
                foreach (var e in _lru)
                    if (e.ExpireAt == 0 || e.ExpireAt > now)
                        list.Add(new SnapEntry { K = e.Key, V = e.Value, E = e.ExpireAt });
                mark = _dirty;
            }
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var tmp = path + ".tmp";
            using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(fs, list);
                fs.Flush(true);
            }
            File.Move(tmp, path, true);
            lock (_sync) { _savedDirty = mark; _lastSaveMs = Now(); }
            return true;
        }
    }

    public int Load(string path)
    {
        if (!File.Exists(path)) return 0;
        List<SnapEntry>? list;
        using (var fs = File.OpenRead(path)) list = JsonSerializer.Deserialize<List<SnapEntry>>(fs);
        if (list == null) return 0;
        int n = 0;
        lock (_sync)
        {
            long now = Now();
            for (int i = list.Count - 1; i >= 0; i--)   // saved MRU->LRU; insert reversed to keep order
            {
                var s = list[i];
                if (s.E != 0 && s.E <= now) continue;
                try { Put(s.K, s.V, s.E); n++; } catch (OomException) { break; }
            }
            _savedDirty = _dirty;
        }
        return n;
    }

    public string Info()
    {
        lock (_sync)
        {
            var sb = new StringBuilder();
            void L(string k, object v) => sb.Append(k).Append(':').Append(Convert.ToString(v, CultureInfo.InvariantCulture)).Append("\r\n");
            L("uptime_seconds", (Now() - _startMs) / 1000);
            L("keys", _map.Count);
            L("keys_with_ttl", _ttl.Count);
            L("used_memory_approx", _used);
            L("maxmemory", _cfg.MaxMemoryMb * 1024L * 1024L);
            L("maxkeys", _cfg.MaxKeys);
            L("eviction_policy", _cfg.EvictionPolicy);
            L("total_commands", Interlocked.Read(ref Commands));
            L("keyspace_hits", _hits);
            L("keyspace_misses", _misses);
            L("expired_keys", _expired);
            L("evicted_keys", _evicted);
            L("last_save_unix_ms", _lastSaveMs);
            return sb.ToString();
        }
    }
}
