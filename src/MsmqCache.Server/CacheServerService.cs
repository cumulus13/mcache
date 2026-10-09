// File: src/MsmqCache.Server/CacheServerService.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Host: snapshot load/save, expiry sweeper, starts TCP and/or MSMQ transports
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

    public CacheServerService(ILogger<CacheServerService> log, ConfigManager cfg, Store store,
                              CommandHandler handler, ServerStats stats)
    { _log = log; _cfg = cfg; _store = store; _handler = handler; _stats = stats; }

    public int TcpPort => _stats.TcpPort;

    protected override async Task ExecuteAsync(CancellationToken stopping)
    {
        await Task.Yield();   // don't block host startup
        var c = _cfg.Current;
        _log.LogInformation("Config: {File}", _cfg.FilePath);

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

        // If any transport dies at startup (port in use, bad ACL...), stop everything and fail the host.
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        Exception? fatal = null;

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
        if (c.EnableTcp)
            tasks.Add(Guard("TCP", () => new TcpTransport(_cfg, _handler, _stats, _log).RunAsync(cts.Token)));
        if (c.EnableMsmq)
            tasks.Add(Guard("MSMQ", () => MsmqTransport.Start(_cfg, _handler, _log, cts.Token)));

        await Task.WhenAll(tasks).ConfigureAwait(false);

        if (_cfg.Current.PersistenceEnabled)
        {
            try { _store.Snapshot(_cfg.SnapshotPath, false); _log.LogInformation("Final snapshot written"); }
            catch (Exception ex) { _log.LogError(ex, "Final snapshot failed"); }
        }

        if (fatal != null) throw fatal;   // makes the host exit non-zero / service report failure
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
