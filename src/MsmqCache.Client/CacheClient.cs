// File: src/MsmqCache.Client/CacheClient.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Synchronous MSMQ request/reply client with per-instance reply queue
// License: MIT

using System.Diagnostics;
using Experimental.System.Messaging;

namespace MsmqCache;

public sealed class CacheClientException : Exception { public CacheClientException(string m) : base(m) { } }

public sealed class CacheClient : IDisposable
{
    readonly MessageQueue _req, _reply;
    readonly string _replyPath;
    readonly TimeSpan _timeout;
    readonly object _lock = new();

    public CacheClient(string requestQueue, TimeSpan timeout)
    {
        _timeout = timeout;
        _replyPath = $@".\Private$\MsmqCacheReply_{Guid.NewGuid():N}";
        _reply = MessageQueue.Create(_replyPath);
        _reply.Formatter = new XmlMessageFormatter(new[] { typeof(string) });
        _req = new MessageQueue(requestQueue);
    }

    public Response Execute(string cmd, params string[] args)
    {
        lock (_lock)
        {
            var req = new Request { Id = Guid.NewGuid().ToString("N"), Cmd = cmd, Args = args };
            using (var m = new Message
            {
                Body = Wire.Pack(req),
                Label = cmd,
                Recoverable = false,
                TimeToBeReceived = _timeout,   // stale requests expire instead of piling up
                ResponseQueue = _reply
            })
                _req.Send(m);

            var sw = Stopwatch.StartNew();
            while (true)
            {
                var left = _timeout - sw.Elapsed;
                if (left <= TimeSpan.Zero) throw new TimeoutException($"No reply to {cmd} within {_timeout.TotalSeconds}s (server down?)");
                Message m2;
                try { m2 = _reply.Receive(left); }
                catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                { throw new TimeoutException($"No reply to {cmd} within {_timeout.TotalSeconds}s (server down?)"); }
                using (m2)
                {
                    var resp = Wire.Unpack<Response>((string)m2.Body);
                    if (resp != null && resp.Id == req.Id) return resp;   // drop late replies from earlier timed-out requests
                }
            }
        }
    }

    // typed helpers
    static Response Check(Response r) => r.Kind == Kinds.Err ? throw new CacheClientException(r.Str ?? "error") : r;

    public string? Get(string key) { var r = Check(Execute("GET", key)); return r.Kind == Kinds.Bulk ? r.Str : null; }

    public bool Set(string key, string value, TimeSpan? ttl = null)
    {
        var args = ttl is { } t ? new[] { key, value, "PX", ((long)t.TotalMilliseconds).ToString() } : new[] { key, value };
        return Check(Execute("SET", args)).Kind == Kinds.Ok;
    }

    public long Del(params string[] keys) => Check(Execute("DEL", keys)).Num;
    public long Incr(string key) => Check(Execute("INCR", key)).Num;

    public void Dispose()
    {
        _req.Dispose();
        _reply.Dispose();
        try { MessageQueue.Delete(_replyPath); } catch { }
    }
}
