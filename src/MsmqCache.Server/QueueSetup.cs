// File: src/MsmqCache.Server/QueueSetup.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Creates/configures the request queue: quotas, journal, authentication, encryption, ACLs
// License: MIT

using Experimental.System.Messaging;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public static class QueueSetup
{
    static readonly object Gate = new();

    // Idempotent: only writes a queue property when it differs from the current value.
    // Security settings (ACL, authentication, encryption) failing is fatal on purpose:
    // the server must not run "open" when the config asked for a locked-down queue.
    public static void Ensure(ServerConfig c, ILogger log)
    {
        lock (Gate)
        {
            if (!MessageQueue.Exists(c.RequestQueue))
            {
                MessageQueue.Create(c.RequestQueue).Dispose();
                log.LogInformation("Created queue {Q}", c.RequestQueue);
            }

            using var q = new MessageQueue(c.RequestQueue);

            if (c.QueueMaxSizeKb > 0 && q.MaximumQueueSize != c.QueueMaxSizeKb)
                q.MaximumQueueSize = c.QueueMaxSizeKb;

            if (q.UseJournalQueue != c.QueueJournal)
                q.UseJournalQueue = c.QueueJournal;
            if (c.QueueJournal && c.JournalMaxSizeKb > 0 && q.MaximumJournalSize != c.JournalMaxSizeKb)
                q.MaximumJournalSize = c.JournalMaxSizeKb;

            if (q.Authenticate != c.QueueAuthenticate)
                q.Authenticate = c.QueueAuthenticate;

            var enc = ParseEncryption(c.QueueEncryption);
            if (q.EncryptionRequired != enc)
                q.EncryptionRequired = enc;

            if (c.ApplyAclOnStartup)
            {
                foreach (var e in c.QueueAcl)
                {
                    q.SetPermissions(new MessageQueueAccessControlEntry(
                        new Trustee(e.Principal), MapRights(e.Rights), AccessControlEntryType.Allow));
                    log.LogInformation("Queue ACL: granted '{Rights}' to {Principal}", e.Rights, e.Principal);
                }
            }
        }
    }

    static EncryptionRequired ParseEncryption(string s) => s switch
    {
        "none" => EncryptionRequired.None,
        "optional" => EncryptionRequired.Optional,
        "body" => EncryptionRequired.Body,
        _ => throw new InvalidDataException("QueueEncryption must be none, optional or body")
    };

    static MessageQueueAccessRights MapRights(string r) => r switch
    {
        "send" => MessageQueueAccessRights.GenericWrite,
        "receive" => MessageQueueAccessRights.GenericRead,
        "full" => MessageQueueAccessRights.FullControl,
        _ => throw new InvalidDataException("Rights must be send, receive or full")
    };
}
