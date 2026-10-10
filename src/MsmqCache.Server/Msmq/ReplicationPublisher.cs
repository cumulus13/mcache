// File: src/MsmqCache.Server/Msmq/ReplicationPublisher.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Primary side: per-replica MSMQ senders (store-and-forward), control listener, full resync, ack monitor
// License: MIT

using System.Collections.Concurrent;
using Experimental.System.Messaging;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class ReplicationPublisher : IDisposable
{
    enum Kind { Entry, Pub, Resync }

    sealed class Item
    {
        public Kind Kind;
        public LogEntry? Entry;
        public string? Channel, Payload;
    }

    const long MaxMessageCost = 2_500_000;
    const int SnapChunkChars = 1_000_000;

    sealed class ReplicaSender
    {
        public readonly string Destination;
        public readonly BlockingCollection<Item> Queue = new(new ConcurrentQueue<Item>());
        public Thread? Thread;
        public int ResyncQueued;
        public ReplicaSender(string dest) => Destination = dest;
    }

    readonly ServerConfig _c;
    readonly Store _store;
    readonly ILogger _log;
    readonly ServerStats _stats;
    readonly List<ReplicaSender> _senders = new();
    readonly List<Thread> _threads = new();
    CancellationToken _ct;
    MessageQueue? _admin;

    public ReplicationPublisher(ServerConfig c, Store store, ILogger log, ServerStats stats)
    {
        _c = c; _store = store; _log = log; _stats = stats;
        foreach (var d in c.Replicas.Where(x => !string.IsNullOrWhiteSpace(x)).Distinct(StringComparer.OrdinalIgnoreCase))
            _senders.Add(new ReplicaSender(d));
    }

    public int ReplicaCount => _senders.Count;
    public long Backlog => _senders.Sum(s => (long)s.Queue.Count);

    public void Start(CancellationToken ct)
    {
        _ct = ct;
        if (_c.ReplicationAcknowledge) { _admin = MsmqUtil.EnsurePlainQueue(_c.AdminQueue); StartThread("repl-admin", AdminLoop); }
        StartThread("repl-control", ControlLoop);
        foreach (var s in _senders)
        {
            var sender = s;
            sender.Thread = new Thread(() => SenderLoop(sender)) { Name = "repl-send", IsBackground = false };
            sender.Thread.Start();
            _log.LogInformation("Replicating to {Dest}", sender.Destination);
        }
    }

    void StartThread(string name, ThreadStart body)
    {
        var t = new Thread(body) { Name = name, IsBackground = false };
        _threads.Add(t); t.Start();
    }

    public void Enqueue(LogEntry e)
    {
        foreach (var s in _senders) Add(s, new Item { Kind = Kind.Entry, Entry = e });
    }

    public void EnqueuePub(string channel, string payload)
    {
        foreach (var s in _senders) Add(s, new Item { Kind = Kind.Pub, Channel = channel, Payload = payload });
    }

    void Add(ReplicaSender s, Item it)
    {
        try { s.Queue.Add(it); } catch (InvalidOperationException) { return; }   // stopping
        if (s.Queue.Count > _c.ReplicationMaxBacklog) DropBacklog(s);
    }

    // The replica is unreachable for too long: stop buffering. It will notice the gap and ask for a full resync.
    void DropBacklog(ReplicaSender s)
    {
        int n = 0;
        while (s.Queue.TryTake(out _)) n++;
        _log.LogError("Replication backlog for {Dest} exceeded {Max}: dropped {N} queued entries; the replica will need a full resync", s.Destination, _c.ReplicationMaxBacklog, n);
    }

    // ---- per-replica sender ----
    void SenderLoop(ReplicaSender s)
    {
        MessageQueue? dest = null;
        try
        {
            var batch = new List<Item>();
            while (!_ct.IsCancellationRequested || s.Queue.Count > 0)
            {
                Item first;
                try { first = s.Queue.Take(_ct); }
                catch (OperationCanceledException) { if (s.Queue.Count == 0) break; if (!s.Queue.TryTake(out first)) break; }

                batch.Clear(); batch.Add(first);
                while (batch.Count < _c.ReplicationBatchMax && batch[^1].Kind != Kind.Resync && s.Queue.TryTake(out var nx)) batch.Add(nx);

                foreach (var body in BuildMessages(s, batch))
                    SendWithRetry(ref dest, s, body.Label, body.Json, body.Priority);
            }
        }
        finally { dest?.Dispose(); }
    }

    sealed record Outgoing(string Label, string Json, MessagePriority Priority);

    IEnumerable<Outgoing> BuildMessages(ReplicaSender s, List<Item> items)
    {
        var current = new ReplMessage { Type = "w", Entries = new List<LogEntry>() };
        long cost = 0;

        Outgoing? Flush()
        {
            if (current.Entries!.Count == 0) return null;
            var o = new Outgoing("w", Wire.Pack(current), MessagePriority.Normal);
            current = new ReplMessage { Type = "w", Entries = new List<LogEntry>() };
            cost = 0;
            return o;
        }

        foreach (var it in items)
        {
            switch (it.Kind)
            {
                case Kind.Entry:
                    {
                        long c = it.Entry!.EstimatedCost();
                        if (current.Entries!.Count > 0 && cost + c > MaxMessageCost) { var f = Flush(); if (f != null) yield return f; }
                        current.Entries!.Add(it.Entry); cost += c;
                        break;
                    }
                case Kind.Pub:
                    {
                        var f = Flush(); if (f != null) yield return f;
                        yield return new Outgoing("pub", Wire.Pack(new ReplMessage { Type = "pub", Channel = it.Channel, Payload = it.Payload }), MessagePriority.Normal);
                        break;
                    }
                case Kind.Resync:
                    {
                        var f = Flush(); if (f != null) yield return f;
                        Interlocked.Exchange(ref s.ResyncQueued, 0);
                        foreach (var o in SnapshotMessages()) yield return o;
                        break;
                    }
            }
        }
        var last = Flush(); if (last != null) yield return last;
    }

    // begin(seq) / chunk... / end(seq): consistent point-in-time copy of the store.
    IEnumerable<Outgoing> SnapshotMessages()
    {
        var (seq, entries) = _store.ExportSnapshot();
        _log.LogInformation("Full resync: sending snapshot at sequence {Seq} ({N} keys)", seq, entries.Count);
        yield return new Outgoing("snap", Wire.Pack(new ReplMessage { Type = "snap", Part = "begin", Seq = seq }), MessagePriority.High);

        var chunk = new List<SnapEntry>(); long chars = 0;
        foreach (var e in entries)
        {
            long c = e.K.Length + e.V.Length + 32;
            if (chunk.Count > 0 && chars + c > SnapChunkChars)
            {
                yield return new Outgoing("snap", Wire.Pack(new ReplMessage { Type = "snap", Part = "chunk", Seq = seq, Items = chunk }), MessagePriority.High);
                chunk = new List<SnapEntry>(); chars = 0;
            }
            chunk.Add(e); chars += c;
        }
        if (chunk.Count > 0)
            yield return new Outgoing("snap", Wire.Pack(new ReplMessage { Type = "snap", Part = "chunk", Seq = seq, Items = chunk }), MessagePriority.High);
        yield return new Outgoing("snap", Wire.Pack(new ReplMessage { Type = "snap", Part = "end", Seq = seq }), MessagePriority.High);
    }

    // One transaction per MSMQ message; retried (with backoff) until accepted by the local MSMQ outgoing queue.
    void SendWithRetry(ref MessageQueue? dest, ReplicaSender s, string label, string json, MessagePriority priority)
    {
        int attempt = 0;
        while (true)
        {
            try
            {
                dest ??= new MessageQueue(s.Destination);
                using var tx = new MessageQueueTransaction();
                tx.Begin();
                try
                {
                    using var m = MsmqUtil.BuildReplicationMessage(json, label, _c, _admin, priority);
                    dest.Send(m, tx);
                    tx.Commit();
                }
                catch { try { tx.Abort(); } catch { } throw; }
                return;
            }
            catch (Exception ex) when (ex is MessageQueueException or InvalidOperationException or ArgumentException)
            {
                if (_ct.IsCancellationRequested && attempt >= 3)
                {
                    _log.LogError("Replication to {Dest} abandoned during shutdown: {Msg}", s.Destination, ex.Message);
                    return;
                }
                _log.LogWarning("Replication send to {Dest} failed ({Msg}); retrying", s.Destination, ex.Message);
                dest?.Dispose(); dest = null;
                try { Task.Delay(Math.Min(30000, 500 << Math.Min(attempt++, 6)), _ct).Wait(); }
                catch (AggregateException) { if (attempt >= 3) return; }
                if (s.Queue.Count > _c.ReplicationMaxBacklog) DropBacklog(s);
            }
        }
    }

    // ---- control queue: replicas ask for a full resync ----
    void ControlLoop()
    {
        while (!_ct.IsCancellationRequested)
        {
            MessageQueue? q = null;
            try
            {
                q = MsmqUtil.EnsureTransactionalQueue(_c.ControlQueue, 0);
                while (!_ct.IsCancellationRequested)
                {
                    using var tx = new MessageQueueTransaction();
                    tx.Begin();
                    Message? m = null;
                    try
                    {
                        try { m = q.Receive(TimeSpan.FromSeconds(1), tx); }
                        catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) { tx.Abort(); continue; }
                        try { HandleControl(m); }
                        catch (Exception ex) { _log.LogError(ex, "Bad control message dropped"); }
                        tx.Commit();
                    }
                    catch { try { tx.Abort(); } catch { } throw; }
                    finally { m?.Dispose(); }
                }
            }
            catch (Exception ex) when (!_ct.IsCancellationRequested)
            {
                _log.LogError(ex, "Control queue error; retrying in 2s");
                _ct.WaitHandle.WaitOne(2000);
            }
            finally { q?.Dispose(); }
        }
    }

    void HandleControl(Message m)
    {
        if (_c.RequireAuthenticatedReplication && !MsmqUtil.IsAuthenticated(m))
        {
            _log.LogWarning("Unauthenticated control message dropped");
            return;
        }
        var rm = Wire.Unpack<ReplMessage>((string)m.Body);
        if (rm == null || rm.Type != "hello" || string.IsNullOrWhiteSpace(rm.ReplicaQueue)) return;

        var s = _senders.FirstOrDefault(x => string.Equals(x.Destination, rm.ReplicaQueue, StringComparison.OrdinalIgnoreCase));
        if (s == null)
        {
            _log.LogWarning("Resync request from unknown replica queue '{Q}' ignored (not in Replicas)", rm.ReplicaQueue);
            return;
        }
        if (Interlocked.Exchange(ref s.ResyncQueued, 1) == 0)
        {
            _log.LogInformation("Replica {Q} (at sequence {Seq}) requested a full resync", s.Destination, rm.Seq);
            try { s.Queue.Add(new Item { Kind = Kind.Resync }); } catch (InvalidOperationException) { }
        }
    }

    // ---- negative acknowledgments from MSMQ for undeliverable replication messages ----
    void AdminLoop()
    {
        while (!_ct.IsCancellationRequested)
        {
            try
            {
                using var m = _admin!.Receive(TimeSpan.FromSeconds(1));
                Interlocked.Increment(ref _stats.ReplicationNacks);
                _log.LogWarning("MSMQ reports a replication delivery problem: {Ack} (message {Corr})", m.Acknowledgment, m.CorrelationId);
            }
            catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) { }
            catch (Exception ex) when (!_ct.IsCancellationRequested)
            {
                _log.LogError(ex, "Admin queue error"); _ct.WaitHandle.WaitOne(2000);
            }
        }
    }

    public void Stop(TimeSpan timeout)
    {
        foreach (var s in _senders) { try { s.Queue.CompleteAdding(); } catch (ObjectDisposedException) { } }
        var deadline = DateTime.UtcNow + timeout;
        foreach (var s in _senders) s.Thread?.Join(Math.Max(100, (int)(deadline - DateTime.UtcNow).TotalMilliseconds));
        foreach (var t in _threads) t.Join(Math.Max(100, (int)(deadline - DateTime.UtcNow).TotalMilliseconds));
    }

    public void Dispose()
    {
        Stop(TimeSpan.FromSeconds(10));
        _admin?.Dispose();
    }
}
