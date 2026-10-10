// File: src/MsmqCache.Server/Msmq/MsmqUtil.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Shared MSMQ helpers: transactional queues, message builders with delivery/security features
// License: MIT

using Experimental.System.Messaging;

namespace MsmqCache;

public static class MsmqUtil
{

    public static XmlMessageFormatter StringFormatter() => new(new[] { typeof(string) });

    // Creates the queue as TRANSACTIONAL when missing; refuses to use an existing non-transactional queue
    // (transactional sends/receives would fail at runtime in a confusing way).
    public static MessageQueue EnsureTransactionalQueue(string path, long maxSizeKb)
    {
        if (!MessageQueue.Exists(path)) MessageQueue.Create(path, true).Dispose();
        var q = new MessageQueue(path);
        try
        {
            if (!q.Transactional)
                throw new InvalidOperationException($"Queue {path} exists but is not transactional. Delete it or choose another name.");
            if (maxSizeKb > 0 && q.MaximumQueueSize != maxSizeKb) q.MaximumQueueSize = maxSizeKb;
            q.Formatter = StringFormatter();
            return q;
        }
        catch { q.Dispose(); throw; }
    }

    public static MessageQueue EnsurePlainQueue(string path)
    {
        if (!MessageQueue.Exists(path)) MessageQueue.Create(path, false).Dispose();
        var q = new MessageQueue(path);
        q.Formatter = StringFormatter();
        return q;
    }

    // Replication / control message with the configured MSMQ delivery features.
    public static Message BuildReplicationMessage(string body, string label, ServerConfig c, MessageQueue? adminQueue,
                                                  MessagePriority priority = MessagePriority.Normal)
    {
        var m = new Message
        {
            Body = body,
            Label = label,
            Recoverable = true,
            Priority = priority,
            UseDeadLetterQueue = c.ReplicationUseDeadLetterQueue,
            UseJournalQueue = c.ReplicationJournal,
            UseAuthentication = c.ReplicationAuthenticate,
            AttachSenderId = c.ReplicationAuthenticate,
            UseEncryption = c.ReplicationEncrypt
        };
        if (c.ReplicationTimeToReachQueueSeconds > 0)
            m.TimeToReachQueue = TimeSpan.FromSeconds(c.ReplicationTimeToReachQueueSeconds);
        if (adminQueue != null)
        {
            m.AdministrationQueue = adminQueue;
            m.AcknowledgeType = AcknowledgeTypes.NotAcknowledgeReachQueue | AcknowledgeTypes.NotAcknowledgeReceive;
        }
        return m;
    }

    public static bool IsAuthenticated(Message m)
    {
        try { return m.Authenticated; } catch { return false; }
    }

    // "<machine or .>\private$\name" -> "private$\name" (what MessageQueue.QueueName reports)
    public static string QueueNameOf(string path)
    {
        int i = path.IndexOf('\\');
        return i >= 0 ? path[(i + 1)..] : path;
    }
}
