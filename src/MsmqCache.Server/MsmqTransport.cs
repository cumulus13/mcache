// File: src/MsmqCache.Server/MsmqTransport.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Optional MSMQ request/reply transport (Windows only)
// License: MIT

using System.Runtime.CompilerServices;
using System.Text.Json;
using Experimental.System.Messaging;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class MsmqTransport
{
    readonly ILogger _log;
    readonly ConfigManager _cfg;
    readonly CommandHandler _handler;

    public MsmqTransport(ConfigManager cfg, CommandHandler handler, ILogger log)
    { _cfg = cfg; _handler = handler; _log = log; }

    public async Task RunAsync(CancellationToken ct)
    {
        await Task.Yield();
        var c = _cfg.Current;
        QueueSetup.Ensure(c, _log);

        var tasks = new List<Task>();
        for (int i = 0; i < c.WorkerThreads; i++)
        {
            int id = i;
            tasks.Add(Task.Factory.StartNew(() => WorkerLoop(id, ct), CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default));
        }
        _log.LogInformation("MSMQ listening on {Q} with {N} workers", c.RequestQueue, c.WorkerThreads);
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    void WorkerLoop(int id, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            MessageQueue? q = null;
            try
            {
                QueueSetup.Ensure(_cfg.Current, _log);
                // MessageQueue instances are not thread-safe: one per worker.
                q = new MessageQueue(_cfg.Current.RequestQueue)
                {
                    Formatter = new XmlMessageFormatter(new[] { typeof(string) })
                };
                while (!ct.IsCancellationRequested)
                {
                    Message msg;
                    try { msg = q.Receive(TimeSpan.FromSeconds(1)); }
                    catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) { continue; }
                    using (msg) Handle(msg);
                }
            }
            catch (Exception ex)
            {
                _log.LogError(ex, "MSMQ worker {Id} error; retrying in 2s", id);
                ct.WaitHandle.WaitOne(2000);
            }
            finally { q?.Dispose(); }
        }
    }

    void Handle(Message msg)
    {
        Request? req = null;
        Response resp;
        try
        {
            req = Wire.Unpack<Request>((string)msg.Body) ?? throw new JsonException("null request");
            resp = _cfg.Current.RequireAuthenticatedRequests && !IsAuthenticated(msg)
                ? Response.Error("authentication required")
                : _handler.ExecuteAsync(req, CancellationToken.None).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            _log.LogWarning("Malformed request: {Msg}", ex.Message);
            resp = Response.Error("malformed request");
        }
        resp.Id = req?.Id ?? "";

        MessageQueue? rq = msg.ResponseQueue;
        if (rq == null) return;   // fire-and-forget request
        using (rq)
        {
            try { SendReply(rq, resp); }
            catch (MessageQueueException ex)
            {
                _log.LogWarning("Reply failed: {Code}", ex.MessageQueueErrorCode);
                try { SendReply(rq, new Response { Id = resp.Id, Kind = Kinds.Err, Str = "reply failed: " + ex.MessageQueueErrorCode }); } catch { }
            }
        }
    }

    static bool IsAuthenticated(Message msg)
    {
        try { return msg.Authenticated; } catch { return false; }
    }

    void SendReply(MessageQueue rq, Response r)
    {
        using var m = new Message
        {
            Body = Wire.Pack(r),
            Label = "resp",
            Recoverable = _cfg.Current.ReplyRecoverable,
            UseDeadLetterQueue = _cfg.Current.UseDeadLetterQueue,
            TimeToBeReceived = TimeSpan.FromSeconds(_cfg.Current.ReplyTtlSeconds)
        };
        rq.Send(m);
    }

    // Separate non-inlined factory so the MSMQ assembly is only touched when EnableMsmq is true.
    [MethodImpl(MethodImplOptions.NoInlining)]
    public static Task Start(ConfigManager cfg, CommandHandler handler, ILogger log, CancellationToken ct)
        => new MsmqTransport(cfg, handler, log).RunAsync(ct);
}
