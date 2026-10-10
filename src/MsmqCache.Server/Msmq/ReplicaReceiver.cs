// File: src/MsmqCache.Server/Msmq/ReplicaReceiver.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Replica side: transactional MSMQ consumer, idempotent apply, gap detection, full resync
// License: MIT

using Experimental.System.Messaging;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class ReplicaReceiver : IDisposable
{
    readonly ServerConfig _c;
    readonly Store _store;
    readonly PubSubHub _hub;
    readonly ILogger _log;
    readonly ServerStats _stats;
    Thread? _thread;
    CancellationToken _ct;
    MessageQueue? _admin;

    // Replication state (only touched by the receiver thread)
    volatile bool _awaitingSnapshot;
    bool _receivingSnapshot;
    long _snapSeq;
    List<SnapEntry>? _snapBuf;
    DateTime _lastHello = DateTime.MinValue;

    public ReplicaReceiver(ServerConfig c, Store store, PubSubHub hub, ILogger log, ServerStats stats)
    { _c = c; _store = store; _hub = hub; _log = log; _stats = stats; }

    public bool AwaitingSnapshot => _awaitingSnapshot;

    public void Start(CancellationToken ct)
    {
        _ct = ct;
        if (_c.ReplicationAcknowledge) _admin = MsmqUtil.EnsurePlainQueue(_c.AdminQueue);
        _thread = new Thread(Loop) { Name = "replica-receiver", IsBackground = false };
        _thread.Start();
        _log.LogInformation("Replica consuming {Q}; primary control queue {Ctl}", _c.ReplicationQueue, _c.PrimaryControlQueue);
    }

    void Loop()
    {
        // A replica that has no data yet asks the primary for a full copy right away.
        if (_store.Seq == 0) { _awaitingSnapshot = true; TrySendHello(force: true); }

        while (!_ct.IsCancellationRequested)
        {
            MessageQueue? q = null;
            try
            {
                q = MsmqUtil.EnsureTransactionalQueue(_c.ReplicationQueue, 0);
                while (!_ct.IsCancellationRequested)
                {
                    using var tx = new MessageQueueTransaction();
                    tx.Begin();
                    Message? m = null;
                    try
                    {
                        try { m = q.Receive(TimeSpan.FromSeconds(1), tx); }
                        catch (MessageQueueException ex) when (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                        {
                            tx.Abort();
                            if (_awaitingSnapshot) TrySendHello(force: false);   // hello lost? ask again after a while
                            continue;
                        }
                        try { Handle(m); }
                        catch (Exception ex) { _log.LogError(ex, "Replication message skipped (poison)"); }
                        tx.Commit();   // apply is idempotent (sequence numbers), so a crash before commit only causes a harmless redelivery
                    }
                    catch { try { tx.Abort(); } catch { } throw; }
                    finally { m?.Dispose(); }
                }
            }
            catch (Exception ex) when (!_ct.IsCancellationRequested)
            {
                _log.LogError(ex, "Replication queue error; retrying in 2s");
                _ct.WaitHandle.WaitOne(2000);
            }
            finally { q?.Dispose(); }
        }
    }

    void Handle(Message m)
    {
        if (_c.RequireAuthenticatedReplication && !MsmqUtil.IsAuthenticated(m))
        {
            _log.LogWarning("Unauthenticated replication message dropped");
            return;
        }
        var rm = Wire.Unpack<ReplMessage>((string)m.Body);
        if (rm == null) return;

        switch (rm.Type)
        {
            case "w": ApplyWrites(rm); break;
            case "pub": if (_c.EnablePubSub && rm.Channel != null) _hub.Publish(rm.Channel, rm.Payload ?? ""); break;
            case "snap": HandleSnapshot(rm); break;
        }
    }

    void ApplyWrites(ReplMessage rm)
    {
        if (rm.Entries == null) return;
        foreach (var e in rm.Entries)
        {
            if (_awaitingSnapshot || _receivingSnapshot) { TrySendHello(force: false); continue; }
            var r = _store.ApplyReplicated(e);
            if (r == ApplyResult.Gap)
            {
                _log.LogWarning("Replication gap: expected sequence {Exp}, got {Got}; requesting a full resync", _store.Seq + 1, e.Seq);
                _awaitingSnapshot = true;
                TrySendHello(force: true);
                return;   // the rest of this message is superseded by the snapshot
            }
        }
    }

    void HandleSnapshot(ReplMessage rm)
    {
        switch (rm.Part)
        {
            case "begin":
                _receivingSnapshot = true; _snapSeq = rm.Seq; _snapBuf = new List<SnapEntry>();
                break;
            case "chunk":
                if (_receivingSnapshot && rm.Seq == _snapSeq && rm.Items != null) _snapBuf!.AddRange(rm.Items);
                break;
            case "end":
                if (_receivingSnapshot && rm.Seq == _snapSeq)
                {
                    _store.InstallSnapshot(_snapSeq, _snapBuf!);
                    _log.LogInformation("Full resync complete: {N} keys at sequence {Seq}", _snapBuf!.Count, _snapSeq);
                    _receivingSnapshot = false; _snapBuf = null; _awaitingSnapshot = false;
                }
                break;
        }
    }

    // replica -> primary control queue (transactional, high priority)
    void TrySendHello(bool force)
    {
        if (!force && (DateTime.UtcNow - _lastHello).TotalSeconds < 60) return;
        _lastHello = DateTime.UtcNow;
        try
        {
            using var dest = new MessageQueue(_c.PrimaryControlQueue);
            using var tx = new MessageQueueTransaction();
            tx.Begin();
            try
            {
                var hello = new ReplMessage { Type = "hello", ReplicaQueue = _c.ReplicaAdvertisedQueue, Seq = _store.Seq };
                using var m = MsmqUtil.BuildReplicationMessage(Wire.Pack(hello), "hello", _c, _admin, MessagePriority.High);
                dest.Send(m, tx);
                tx.Commit();
            }
            catch { try { tx.Abort(); } catch { } throw; }
            _log.LogInformation("Requested a full resync from the primary (local sequence {Seq})", _store.Seq);
        }
        catch (Exception ex) when (ex is MessageQueueException or InvalidOperationException or ArgumentException)
        {
            _log.LogWarning("Could not reach the primary control queue ({Msg}); will retry", ex.Message);
        }
    }

    public void Dispose()
    {
        _thread?.Join(TimeSpan.FromSeconds(5));
        _admin?.Dispose();
    }
}
