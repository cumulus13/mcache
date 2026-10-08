// File: src/MsmqCache.Server/CacheServerService.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: MSMQ worker loop, snapshot load/save, expiry sweeper (BackgroundService, Windows Service ready)
// License: MIT

using System.Diagnostics;
using System.Text.Json;
using Experimental.System.Messaging;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class CacheServerService : BackgroundService
{
    readonly ILogger<CacheServerService> _log;
    readonly ConfigManager _cfg;
    readonly Store _store;
    readonly CommandHandler _handler;

    public CacheServerService(ILogger<CacheServerService> log, ConfigManager cfg, Store store, CommandHandler handler)
    { _log = log; _cfg = cfg; _store = store; _handler = handler; }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await Task.Yield();   // don't block host startup
        var c = _cfg.Current;
        _log.LogInformation("Config: {File}", _cfg.FilePath);
        EnsureQueue(c.RequestQueue);

        if (c.PersistenceEnabled)
        {
            var snap = _cfg.SnapshotPath;
            Directory.CreateDirectory(_cfg.DataDir);
            try { _log.LogInformation("Loaded {N} keys from {P}", _store.Load(snap), snap); }
            catch (Exception ex) when (ex is JsonException or IOException)
            {
                _log.LogError(ex, "Snapshot unreadable, starting empty");
                try { File.Move(snap, snap + ".corrupt-" + DateTime.UtcNow.ToString("yyyyMMddHHmmss")); } catch { }
            }
        }

        var tasks = new List<Task>();
        for (int i = 0; i < c.WorkerThreads; i++)
        {
            int id = i;
            tasks.Add(Task.Factory.StartNew(() => WorkerLoop(id, ct), CancellationToken.None,
                TaskCreationOptions.LongRunning, TaskScheduler.Default));
        }
        tasks.Add(Task.Run(() => Maintenance(ct), CancellationToken.None));
        _log.LogInformation("Listening on {Q} with {N} workers", c.RequestQueue, c.WorkerThreads);

        await Task.WhenAll(tasks);

        if (_cfg.Current.PersistenceEnabled)
        {
            try { _store.Snapshot(_cfg.SnapshotPath, false); _log.LogInformation("Final snapshot written"); }
            catch (Exception ex) { _log.LogError(ex, "Final snapshot failed"); }
        }
    }

    static void EnsureQueue(string path)
    {
        if (!MessageQueue.Exists(path)) MessageQueue.Create(path);
    }

    void WorkerLoop(int id, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            MessageQueue? q = null;
            try
            {
                EnsureQueue(_cfg.Current.RequestQueue);
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
                _log.LogError(ex, "Worker {Id} error; retrying in 2s", id);
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
            resp = _handler.Execute(req);
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

    void SendReply(MessageQueue rq, Response r)
    {
        using var m = new Message
        {
            Body = Wire.Pack(r),
            Label = "resp",
            Recoverable = false,
            TimeToBeReceived = TimeSpan.FromSeconds(_cfg.Current.ReplyTtlSeconds)
        };
        rq.Send(m);
    }

    void Maintenance(CancellationToken ct)
    {
        var sinceSnap = Stopwatch.StartNew();
        while (!ct.WaitHandle.WaitOne(Math.Max(100, _cfg.Current.SweepIntervalMs)))
        {
            try
            {
                _store.Sweep();
                var c = _cfg.Current;
                if (c.PersistenceEnabled && c.SnapshotIntervalSeconds > 0 && sinceSnap.Elapsed.TotalSeconds >= c.SnapshotIntervalSeconds)
                {
                    _store.Snapshot(_cfg.SnapshotPath, false);
                    sinceSnap.Restart();
                }
            }
            catch (Exception ex) { _log.LogError(ex, "Maintenance error"); }
        }
    }
}
