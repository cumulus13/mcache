// File: src/MsmqCache.Client/MsmqCacheClient.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Optional MSMQ request/reply client with per-instance reply queue
// License: MIT

using System.Diagnostics;
using Experimental.System.Messaging;

namespace MsmqCache;

public sealed class MsmqClientOptions
{
    public string RequestQueue { get; set; } = @".\Private$\MsmqCacheRequests";
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);
    public bool Recoverable { get; set; }          // request messages are disk-backed (like your Publisher sample); slower
    public bool UseAuthentication { get; set; }   // sign requests (needs domain/AD or internal certs); also attaches sender id
    public bool UseEncryption { get; set; }        // encrypt request body (queue must allow it)
}

public sealed class MsmqCacheClient : ICacheClient
{
    readonly MessageQueue _req, _reply;
    readonly string _replyPath;
    readonly TimeSpan _timeout;
    readonly MsmqClientOptions _opt;
    readonly object _lock = new();

    public MsmqCacheClient(string requestQueue, TimeSpan timeout)
        : this(new MsmqClientOptions { RequestQueue = requestQueue, Timeout = timeout }) { }

    public MsmqCacheClient(MsmqClientOptions options)
    {
        _opt = options;
        _timeout = options.Timeout;
        _replyPath = $@".\Private$\MsmqCacheReply_{Guid.NewGuid():N}";
        _reply = MessageQueue.Create(_replyPath);
        _reply.Formatter = new XmlMessageFormatter(new[] { typeof(string) });
        _req = new MessageQueue(options.RequestQueue);
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
                Recoverable = _opt.Recoverable,
                UseAuthentication = _opt.UseAuthentication,
                AttachSenderId = _opt.UseAuthentication,
                UseEncryption = _opt.UseEncryption,
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

    public void Dispose()
    {
        _req.Dispose();
        _reply.Dispose();
        try { MessageQueue.Delete(_replyPath); } catch { }
    }
}
