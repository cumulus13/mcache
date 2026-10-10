// File: src/MsmqCache.Server/CacheServerService.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Host: startup (snapshot + WAL replay), transports, replication, maintenance/checkpoints, shutdown
// License: MIT

using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class CacheServerService : BackgroundService
{
    readonly ILogger<CacheServerService> _log;
    readonly ConfigManager _cfg;
    readonly Store _store;
    readonly CommandHandler _handler;
    readonly ServerStats _stats;
    readonly WriteDispatcher _disp;
    readonly PubSubHub _hub;
    readonly Checkpointer _ckpt;

    MsmqWal? _wal;
    ReplicationPublisher? _repl;
    ReplicaReceiver? _replica;

    public CacheServerService(ILogger<CacheServerService> log, ConfigManager cfg, Store store, CommandHandler handler,
                              ServerStats stats, WriteDispatcher disp, PubSubHub hub, Checkpointer ckpt)
    { _log = log; _cfg = cfg; _store = store; _handler = handler; _stats = stats; _disp = disp; _hub = hub; _ckpt = ckpt; }

    public int TcpPort => _stats.TcpPort;

    protected override async Task ExecuteAsync(CancellationToken stopping)
    {
        await Task.Yield();   // don't block host startup
        var c = _cfg.Current;
        _log.LogInformation("Config: {File}", _cfg.FilePath);

        // 1. snapshot, 2. WAL replay (everything newer than the snapshot), 3. attach the write sink.
        if (c.PersistenceEnabled)
        {
            var snap = _cfg.SnapshotPath;
            Directory.CreateDirectory(_cfg.DataDir);
            try { _log.LogInformation("Loaded {N} keys from {P} (sequence {Seq})", _store.Load(snap), snap, _store.Seq); }
            catch (Exception ex) when (ex is JsonException or IOException)
            {
                _log.LogError(ex, "Snapshot unreadable, starting empty");
                try { File.Move(snap, snap + ".corrupt-" + DateTime.UtcNow.ToString("yyyyMMddHHmmss")); } catch { }
            }
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        Exception? fatal = null;

        try
        {
            if (c.WalEnabled)
            {
                _wal = new MsmqWal(c, _log, _stats);
                _wal.Replay(_store);
                _wal.Start(cts.Token, _store.Seq);
                _disp.Wal = _wal;
            }
            _store.SetSink(_disp);

            if (c.Role == "replica")
            {
                _store.SnapshotInstalled += _ckpt.Request;   // a full resync replaces the data: checkpoint (and reset the WAL) right away
                _replica = new ReplicaReceiver(c, _store, _hub, _log, _stats);
                _replica.Start(cts.Token);
            }
            else if (c.Replicas.Length > 0)
            {
                _repl = new ReplicationPublisher(c, _store, _log, _stats);
                _disp.Repl = _repl;
                _repl.Start(cts.Token);
            }
        }
        catch (Exception ex)
        {
            _log.LogCritical(ex, "Startup failed");
            fatal = ex;
            cts.Cancel();
        }

        async Task Guard(string name, Func<Task> run)
        {
            try { await run().ConfigureAwait(false); }
            catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
            catch (Exception ex)
            {
                _log.LogCritical(ex, "{Name} transport failed", name);
                fatal ??= ex;
                cts.Cancel();
            }
        }

        var tasks = new List<Task> { Task.Run(() => Maintenance(cts.Token), CancellationToken.None) };
        if (fatal == null)
        {
            if (c.EnableTcp)
                tasks.Add(Guard("TCP", () => new TcpTransport(_cfg, _handler, _hub, _stats, _log).RunAsync(cts.Token)));
            if (c.EnableMsmq)
                tasks.Add(Guard("MSMQ", () => MsmqTransport.Start(_cfg, _handler, _log, cts.Token)));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        // Shutdown: transports are down. Drain WAL + replication, then a last snapshot.
        _store.SetSink(null);
        try { _wal?.Stop(TimeSpan.FromSeconds(10)); } catch (Exception ex) { _log.LogError(ex, "WAL stop failed"); }
        try { _repl?.Stop(TimeSpan.FromSeconds(10)); } catch (Exception ex) { _log.LogError(ex, "Replication stop failed"); }
        try { _replica?.Dispose(); } catch (Exception ex) { _log.LogError(ex, "Replica stop failed"); }
        try { _ckpt.Final(); _log.LogInformation("Final snapshot written"); }
        catch (Exception ex) { _log.LogError(ex, "Final snapshot failed"); }

        if (fatal != null) throw fatal;   // makes the host exit non-zero / the service report failure
    }

    void Maintenance(CancellationToken ct)
    {
        var sinceSnap = Stopwatch.StartNew();
        while (!ct.WaitHandle.WaitOne(Math.Max(100, _cfg.Current.SweepIntervalMs)))
        {
            try
            {
                _store.Sweep();
                if (_ckpt.ShouldRun(sinceSnap.Elapsed.TotalSeconds))
                {
                    _ckpt.Run(force: false);
                    sinceSnap.Restart();
                }
            }
            catch (Exception ex) { _log.LogError(ex, "Maintenance error"); }
        }
    }
}
