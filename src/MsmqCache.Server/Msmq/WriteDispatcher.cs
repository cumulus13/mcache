// File: src/MsmqCache.Server/Msmq/WriteDispatcher.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Fans store writes out to the WAL and to replicas (sink called under the store lock; never blocks)
// License: MIT

namespace MsmqCache;

public sealed class WriteDispatcher : IWriteSink
{
    // Set by the host once the components exist; null when the feature is off.
    public volatile MsmqWal? Wal;
    public volatile ReplicationPublisher? Repl;

    public void Append(LogEntry entry, bool replicate)
    {
        Wal?.Enqueue(entry);
        if (replicate) Repl?.Enqueue(entry);
    }

    public void Publish(string channel, string payload) => Repl?.EnqueuePub(channel, payload);
}
