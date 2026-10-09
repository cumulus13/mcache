// File: src/MsmqCache.Client/ICacheClient.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Client abstraction + typed helpers shared by TCP and MSMQ clients
// License: MIT

namespace MsmqCache;

public sealed class CacheClientException : Exception
{
    public CacheClientException(string m) : base(m) { }
}

public interface ICacheClient : IDisposable
{
    Response Execute(string cmd, params string[] args);
}

public static class CacheClientExtensions
{
    static Response Check(Response r) =>
        r.Kind == Kinds.Err ? throw new CacheClientException(r.Str ?? "error") : r;

    public static bool Ping(this ICacheClient c) => Check(c.Execute("PING")).Kind == Kinds.Ok;

    public static string? Get(this ICacheClient c, string key)
    {
        var r = Check(c.Execute("GET", key));
        return r.Kind == Kinds.Bulk ? r.Str : null;
    }

    public static bool Set(this ICacheClient c, string key, string value, TimeSpan? ttl = null)
    {
        var args = ttl is { } t
            ? new[] { key, value, "PX", ((long)t.TotalMilliseconds).ToString() }
            : new[] { key, value };
        return Check(c.Execute("SET", args)).Kind == Kinds.Ok;
    }

    public static long Del(this ICacheClient c, params string[] keys) => Check(c.Execute("DEL", keys)).Num;
    public static long Incr(this ICacheClient c, string key) => Check(c.Execute("INCR", key)).Num;
    public static long Ttl(this ICacheClient c, string key) => Check(c.Execute("TTL", key)).Num;
}
