// File: src/MsmqCache.Server/CommandHandler.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Command dispatch and argument validation
// License: MIT

using System.Globalization;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class CommandHandler
{
    static readonly CultureInfo Inv = CultureInfo.InvariantCulture;
    readonly Store _s;
    readonly ConfigManager _c;
    readonly ILogger<CommandHandler> _log;
    readonly HashSet<string> _disabled;

    public CommandHandler(Store s, ConfigManager c, ILogger<CommandHandler> log)
    {
        _s = s; _c = c; _log = log;
        _disabled = new(c.Current.DisabledCommands.Select(x => x.Trim().ToUpperInvariant()), StringComparer.Ordinal);
    }

    public Response Execute(Request r)
    {
        Interlocked.Increment(ref _s.Commands);
        string cmd = (r.Cmd ?? "").Trim().ToUpperInvariant();
        var a = r.Args ?? Array.Empty<string>();
        try
        {
            if (_disabled.Contains(cmd)) throw new CacheException($"command '{cmd}' is disabled");
            return Dispatch(cmd, a);
        }
        catch (CacheException ex) { return Response.Error(ex.Message); }
        catch (Exception ex)
        {
            _log.LogError(ex, "Command {Cmd} failed", cmd);
            return Response.Error("internal error: " + ex.GetType().Name);
        }
    }

    Response Dispatch(string cmd, string[] a)
    {
        switch (cmd)
        {
            case "PING": Arity(cmd, a, 0, 1); return a.Length == 0 ? Response.Ok("PONG") : Response.Bulk(a[0]);
            case "GET": { Arity(cmd, a, 1, 1); var v = _s.Get(Key(a[0])); return v == null ? Response.Nil() : Response.Bulk(v); }
            case "SET": return Set(a);
            case "MGET": Arity(cmd, a, 1, int.MaxValue); return Response.List(_s.MGet(a.Select(Key).ToArray()));
            case "MSET":
                if (a.Length < 2 || a.Length % 2 != 0) throw new CacheException("wrong number of arguments for 'MSET'");
                for (int i = 0; i < a.Length; i += 2) { Key(a[i]); Val(a[i + 1]); }
                for (int i = 0; i < a.Length; i += 2) _s.Set(a[i], a[i + 1], 0, false, false);
                return Response.Ok();
            case "DEL": Arity(cmd, a, 1, int.MaxValue); return Response.Number(_s.Del(a));
            case "EXISTS": Arity(cmd, a, 1, int.MaxValue); return Response.Number(_s.Exists(a));
            case "INCR": Arity(cmd, a, 1, 1); return Response.Number(_s.Incr(Key(a[0]), 1));
            case "DECR": Arity(cmd, a, 1, 1); return Response.Number(_s.Incr(Key(a[0]), -1));
            case "INCRBY": Arity(cmd, a, 2, 2); return Response.Number(_s.Incr(Key(a[0]), Long(a[1])));
            case "DECRBY": Arity(cmd, a, 2, 2); return Response.Number(_s.Incr(Key(a[0]), checked(-Long(a[1]))));
            case "EXPIRE": Arity(cmd, a, 2, 2); return Response.Number(_s.Expire(Key(a[0]), Long(a[1])) ? 1 : 0);
            case "TTL": Arity(cmd, a, 1, 1); return Response.Number(_s.Ttl(Key(a[0])));
            case "PERSIST": Arity(cmd, a, 1, 1); return Response.Number(_s.Persist(Key(a[0])) ? 1 : 0);
            case "KEYS": Arity(cmd, a, 1, 1); return Response.List(_s.Keys(a[0]));
            case "DBSIZE": Arity(cmd, a, 0, 0); return Response.Number(_s.DbSize());
            case "FLUSHALL": Arity(cmd, a, 0, 0); _s.FlushAll(); return Response.Ok();
            case "SAVE":
                Arity(cmd, a, 0, 0);
                if (!_c.Current.PersistenceEnabled) throw new CacheException("persistence is disabled");
                _s.Snapshot(_c.SnapshotPath, true);
                return Response.Ok();
            case "INFO":
                Arity(cmd, a, 0, 0);
                return Response.Bulk($"config_file:{_c.FilePath}\r\nrequest_queue:{_c.Current.RequestQueue}\r\n" + _s.Info());
            case "CONFIG": return Config(a);
            default: throw new CacheException($"unknown command '{cmd}'");
        }
    }

    Response Set(string[] a)
    {
        Arity("SET", a, 2, int.MaxValue);
        string key = Key(a[0]), val = Val(a[1]);
        long ttl = 0; bool nx = false, xx = false;
        for (int i = 2; i < a.Length; i++)
        {
            switch (a[i].ToUpperInvariant())
            {
                case "EX": ttl = Ttl(a, ++i, 1000); break;
                case "PX": ttl = Ttl(a, ++i, 1); break;
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                default: throw new CacheException("syntax error");
            }
        }
        if (nx && xx) throw new CacheException("syntax error");
        return _s.Set(key, val, ttl, nx, xx) ? Response.Ok() : Response.Nil();
    }

    Response Config(string[] a)
    {
        if (a.Length < 2) throw new CacheException("wrong number of arguments for 'CONFIG'");
        switch (a[0].ToUpperInvariant())
        {
            case "GET":
                {
                    var want = a[1].ToLowerInvariant();
                    var list = new List<string?>();
                    foreach (var kv in _c.All())
                        if (want == "*" || kv.Key == want) { list.Add(kv.Key); list.Add(kv.Value); }
                    return Response.List(list.ToArray());
                }
            case "SET":
                {
                    if (a.Length != 3) throw new CacheException("wrong number of arguments for 'CONFIG SET'");
                    var err = _c.TrySet(a[1], a[2]);
                    if (err != null) throw new CacheException(err);
                    return Response.Ok();
                }
            default: throw new CacheException("syntax error");
        }
    }

    // ---- validation helpers ----
    static void Arity(string cmd, string[] a, int min, int max)
    {
        if (a.Length < min || a.Length > max) throw new CacheException($"wrong number of arguments for '{cmd}'");
    }

    string Key(string k)
    {
        if (string.IsNullOrEmpty(k) || k.Length > _c.Current.MaxKeyLength) throw new CacheException("invalid key length");
        return k;
    }

    string Val(string v)
    {
        if (v == null || v.Length > _c.Current.MaxValueLength) throw new CacheException("value too large");
        return v;
    }

    static long Long(string s)
    {
        if (!long.TryParse(s, NumberStyles.Integer, Inv, out var n)) throw new CacheException("value is not an integer or out of range");
        return n;
    }

    static long Ttl(string[] a, int i, long mult)
    {
        if (i >= a.Length || !long.TryParse(a[i], NumberStyles.None, Inv, out var n) || n <= 0)
            throw new CacheException("invalid expire time");
        try { return checked(n * mult); } catch (OverflowException) { throw new CacheException("invalid expire time"); }
    }
}
