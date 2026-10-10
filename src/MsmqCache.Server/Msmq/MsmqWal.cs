// File: src/MsmqCache.Server/Msmq/MsmqWal.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Write-ahead log in transactional MSMQ segment queues: group commit, sync/async, replay, rotate/truncate
// License: MIT

using System.Collections.Concurrent;
using System.Text.Json;
using Experimental.System.Messaging;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class MsmqWal : IDisposable
{
    sealed class Item
    {
        public LogEntry? Entry;
        public TaskCompletionSource<(long Boundary, int Segment)>? Rotate;
    }

    const long MaxMessageCost = 2_500_000;   // estimated JSON chars per MSMQ message (hard limit is 4 MB)

    readonly ServerConfig _c;
    readonly ILogger _log;
    readonly ServerStats _stats;
    readonly BlockingCollection<Item> _q = new(new ConcurrentQueue<Item>());
    readonly object _wl = new();
    readonly List<(long Seq, TaskCompletionSource Tcs)> _waiters = new();
    Thread? _thread;
    MessageQueue? _out;
    CancellationToken _ct;
    long _durable, _lastWritten;
    volatile int _segment;

    public MsmqWal(ServerConfig c, ILogger log, ServerStats stats) { _c = c; _log = log; _stats = stats; }

    public long DurableSeq => Interlocked.Read(ref _durable);
    public int Backlog => _q.Count;
    public int CurrentSegment => _segment;

    string SegmentPath(int n) => $"{_c.WalQueue}_{n:D8}";

    // ---- segments ----
    List<(int Number, string Path)> ListSegments()
    {
        var res = new List<(int, string)>();
        string prefix = MsmqUtil.QueueNameOf(_c.WalQueue) + "_";
        try
        {
            foreach (var q in MessageQueue.GetPrivateQueuesByMachine("."))
            {
                using (q)
                {
                    string name = q.QueueName;
                    if (name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) &&
                        int.TryParse(name[prefix.Length..], out int n))
                        res.Add((n, SegmentPath(n)));
                }
            }
        }
        catch (MessageQueueException ex)
        {
            // Enumerating queues needs management rights; fall back to probing segment numbers.
            _log.LogWarning("Cannot enumerate queues ({Code}); probing WAL segments 1..2000", ex.MessageQueueErrorCode);
            for (int n = 1; n <= 2000; n++)
                if (MessageQueue.Exists(SegmentPath(n))) res.Add((n, SegmentPath(n)));
        }
        res.Sort((a, b) => a.Item1.CompareTo(b.Item1));
        return res;
    }

    MessageQueue OpenSegment(int n) => MsmqUtil.EnsureTransactionalQueue(SegmentPath(n), _c.WalQueueMaxSizeKb);

    // ---- startup: replay everything newer than the store's current (snapshot) sequence ----
    public int Replay(Store store)
    {
        var segs = ListSegments();
        int applied = 0;
        foreach (var (n, path) in segs)
        {
            using var q = new MessageQueue(path) { Formatter = MsmqUtil.StringFormatter() };
            using var en = q.GetMessageEnumerator2();
            while (en.MoveNext(TimeSpan.Zero))
            {
                using var m = en.Current;
                try
                {
                    var batch = Wire.Unpack<WalBatch>((string)m.Body) ?? throw new JsonException("empty WAL batch");
                    foreach (var e in batch.Entries)
                        if (store.ReplayEntry(e)) applied++;
                }
                catch (Exception ex) when (ex is JsonException or InvalidOperationException or InvalidCastException or InvalidDataException)
                {
                    if (!_c.WalSkipCorrupt)
                        throw new InvalidDataException($"Corrupt WAL message {m.Id} in {path}: {ex.Message}. " +
                                                       "Fix or delete the segment, or set WalSkipCorrupt=true to skip corrupt messages.", ex);
                    _log.LogError(ex, "Skipping corrupt WAL message {Id} in {Path}", m.Id, path);
                }
            }
        }
        _segment = segs.Count > 0 ? segs[^1].Number : 0;
        _log.LogInformation("WAL replay: {N} entries applied from {S} segment(s); store sequence {Seq}", applied, segs.Count, store.Seq);
        return applied;
    }

    public void Start(CancellationToken ct, long startSeq)
    {
        _ct = ct;
        _lastWritten = startSeq;
        Interlocked.Exchange(ref _durable, startSeq);
        if (_segment == 0) _segment = 1;
        _out = OpenSegment(_segment);
        _thread = new Thread(Loop) { Name = "wal-writer", IsBackground = false };
        _thread.Start();
        _log.LogInformation("WAL writing to {Q}_{Seg:D8} (mode {Mode})", _c.WalQueue, _segment, _c.WalMode);
    }

    // ---- producers ----
    public void Enqueue(LogEntry e)
    {
        try { _q.Add(new Item { Entry = e }); } catch (InvalidOperationException) { /* shutting down */ }
    }

    public Task WaitDurableAsync(long seq)
    {
        lock (_wl)
        {
            if (DurableSeq >= seq) return Task.CompletedTask;
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((seq, tcs));
            return tcs.Task;
        }
    }

    // Switches writing to a new segment. Everything enqueued before this call lands in older segments; returns the last such sequence.
    public (long Boundary, int Segment) Rotate(TimeSpan timeout)
    {
        var item = new Item { Rotate = new TaskCompletionSource<(long, int)>(TaskCreationOptions.RunContinuationsAsynchronously) };
        _q.Add(item);
        if (!item.Rotate.Task.Wait(timeout)) throw new TimeoutException("WAL rotation timed out (is MSMQ healthy?)");
        return item.Rotate.Task.Result;
    }

    public int DeleteSegmentsBefore(int segment)
    {
        int deleted = 0;
        foreach (var (n, path) in ListSegments())
        {
            if (n >= segment) continue;
            try { MessageQueue.Delete(path); deleted++; }
            catch (MessageQueueException ex) { _log.LogWarning("Could not delete WAL segment {Path}: {Code}", path, ex.MessageQueueErrorCode); }
        }
        if (deleted > 0) _log.LogInformation("WAL: dropped {N} old segment(s) (< {Seg})", deleted, segment);
        return deleted;
    }

    // ---- writer thread ----
    void Loop()
    {
        while (true)
        {
            Item first;
            try { first = _q.Take(); }
            catch (InvalidOperationException) { break; }   // completed and drained

            var items = new List<Item> { first };
            while (items.Count < _c.WalBatchMax && first.Rotate == null && _q.TryTake(out var nx))
            {
                items.Add(nx);
                if (nx.Rotate != null) break;
            }
            try { Process(items); }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _stats.WalErrors);
                _log.LogCritical(ex, "WAL writer failed; {N} entries may not be durable", items.Count);
            }
        }
    }

    void Process(List<Item> items)
    {
        var pending = new List<LogEntry>();
        foreach (var it in items)
        {
            if (it.Entry != null) { pending.Add(it.Entry); continue; }
            WriteEntries(pending); pending.Clear();
            DoRotate(it.Rotate!);
        }
        WriteEntries(pending);
    }

    void DoRotate(TaskCompletionSource<(long, int)> tcs)
    {
        try
        {
            int next = _segment + 1;
            var nq = OpenSegment(next);
            var old = _out;
            _out = nq; _segment = next;
            old?.Dispose();
            tcs.SetResult((_lastWritten, next));
        }
        catch (Exception ex) { tcs.SetException(ex); }
    }

    // Groups entries into as few MSMQ messages as fit, sends them in ONE transaction (all or nothing), retries until it works.
    void WriteEntries(List<LogEntry> entries)
    {
        if (entries.Count == 0) return;

        var bodies = new List<string>();
        var batch = new WalBatch();
        long cost = 0;
        foreach (var e in entries)
        {
            long c = e.EstimatedCost();
            if (batch.Entries.Count > 0 && cost + c > MaxMessageCost)
            {
                bodies.Add(Wire.Pack(batch)); batch = new WalBatch(); cost = 0;
            }
            batch.Entries.Add(e); cost += c;
        }
        bodies.Add(Wire.Pack(batch));
        long lastSeq = entries[^1].Seq;

        int attempt = 0;
        var stopAt = DateTime.MaxValue;
        while (true)
        {
            try
            {
                using var tx = new MessageQueueTransaction();
                tx.Begin();
                try
                {
                    foreach (var body in bodies)
                    {
                        using var m = new Message { Body = body, Label = "wal", Recoverable = true };
                        _out!.Send(m, tx);
                    }
                    tx.Commit();
                }
                catch { try { tx.Abort(); } catch { } throw; }
                break;
            }
            catch (Exception ex) when (ex is MessageQueueException or InvalidOperationException)
            {
                Interlocked.Increment(ref _stats.WalErrors);
                if (_ct.IsCancellationRequested && stopAt == DateTime.MaxValue) stopAt = DateTime.UtcNow.AddSeconds(5);
                if (DateTime.UtcNow > stopAt)
                {
                    _log.LogCritical(ex, "WAL write abandoned during shutdown; the last {N} entries are not durable", entries.Count);
                    return;
                }
                _log.LogError("WAL write failed ({Msg}); retrying", ex.Message);
                Thread.Sleep(Math.Min(5000, 100 << Math.Min(attempt++, 6)));
                try { _out?.Dispose(); _out = OpenSegment(_segment); } catch { /* retry */ }
            }
        }

        _lastWritten = lastSeq;
        lock (_wl)
        {
            Interlocked.Exchange(ref _durable, Math.Max(DurableSeq, lastSeq));
            for (int i = _waiters.Count - 1; i >= 0; i--)
                if (_waiters[i].Seq <= DurableSeq) { _waiters[i].Tcs.TrySetResult(); _waiters.RemoveAt(i); }
        }
    }

    // Drains the queue, then stops the writer.
    public void Stop(TimeSpan timeout)
    {
        try { _q.CompleteAdding(); } catch (ObjectDisposedException) { }
        if (_thread != null && !_thread.Join(timeout)) _log.LogError("WAL writer did not stop within {T}", timeout);
    }

    public void Dispose()
    {
        Stop(TimeSpan.FromSeconds(10));
        _out?.Dispose();
        _q.Dispose();
    }
}
