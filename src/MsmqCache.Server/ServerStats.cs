// File: src/MsmqCache.Server/ServerStats.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Shared runtime counters (clients, connections, bound port)
// License: MIT

namespace MsmqCache;

public sealed class ServerStats
{
    public long ConnectedClients;
    public long TotalConnections;
    public long RejectedConnections;
    public long NextClientId;
    public long ReplicationNacks;
    public long WalErrors;
    public volatile int TcpPort;     // actual bound port (useful when Port = 0)
}
