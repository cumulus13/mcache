// File: src/MsmqCache.Server/Checkpointer.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Snapshot + WAL segment rotation/truncation (periodic, size-triggered, SAVE, replica full sync)
// License: MIT

using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class Checkpointer
{
    readonly ConfigManager _cfg;
    readonly Store _store;
    readonly WriteDispatcher _dispatcher;
    readonly ILogger<Checkpointer> _log;
    readonly object _gate = new();
    volatile bool _requested;

    public Checkpointer(ConfigManager cfg, Store store, WriteDispatcher dispatcher, ILogger<Checkpointer> log)
    { _cfg = cfg; _store = store; _dispatcher = dispatcher; _log = log; }

    public void Request() => _requested = true;

    public bool ShouldRun(double secondsSinceLast)
    {
        var c = _cfg.Current;
        if (!c.PersistenceEnabled) return false;
        if (_requested) return true;
        if (c.SnapshotIntervalSeconds > 0 && secondsSinceLast >= c.SnapshotIntervalSeconds) return true;
        return _dispatcher.Wal != null && _store.SeqSinceSnapshot >= c.WalCheckpointEntries;
    }

    // With a WAL: rotate to a fresh segment, snapshot, then delete the segments the snapshot covers.
    // Without: plain snapshot (skipped when nothing changed unless force).
    public bool Run(bool force)
    {
        lock (_gate)
        {
            var c = _cfg.Current;
            if (!c.PersistenceEnabled) return false;
            var wal = _dispatcher.Wal;
            bool forced = force || _requested || wal != null;
            _requested = false;

            int segment = 0;
            if (wal != null) segment = wal.Rotate(TimeSpan.FromSeconds(30)).Segment;
            bool wrote = _store.Snapshot(_cfg.SnapshotPath, forced, segment);
            if (wal != null && wrote && segment > 0) wal.DeleteSegmentsBefore(segment);
            if (wrote) _log.LogDebug("Checkpoint written (sequence {Seq})", _store.Seq);
            return wrote;
        }
    }

    // Shutdown: the WAL is already drained; one last snapshot without touching segments.
    public void Final()
    {
        lock (_gate)
        {
            if (!_cfg.Current.PersistenceEnabled) return;
            _store.Snapshot(_cfg.SnapshotPath, false, _dispatcher.Wal?.CurrentSegment ?? 0);
        }
    }
}
