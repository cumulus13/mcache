# msmqcache - Production Guide

By Hadi Cahyadi - version 1.0 - 2026-10-09

## 1. What it is

**msmqcache** is a Redis-compatible cache server for .NET 8 that uses **MSMQ as its durability and replication backend**.

- Clients connect over **TCP (default 6379)** with `redis-cli` or any Redis library (RESP2).
- Data lives in memory (strings only: TTL, LRU, memory limits, atomic snapshots).
- **MSMQ is the backend**, in four roles:
  1. **Write-ahead log (WAL):** every write is appended to transactional MSMQ queues and replayed at startup.
  2. **Replication:** writes stream to replicas through MSMQ (store-and-forward, exactly-once, full-resync fallback).
  3. **Pub/Sub bus:** `PUBLISH` is forwarded to replicas, so subscribers on any node get the message.
  4. **Request/reply door:** an optional second way for Windows clients to send commands over a queue.

MSMQ is **not** the key/value storage: a queue has no lookup by key, no in-place update and no per-key TTL, so it cannot serve `GET` efficiently.

> **Status:** syntax-checked and test-covered, but never compiled or run (the build sandbox had no .NET SDK and MSMQ is Windows-only). First steps: `dotnet build`, `dotnet test`, `python scripts/smoke_test.py`, then the MSMQ tests (`integration-msmq` workflow or `MSMQCACHE_MSMQ_TESTS=1`).

## 2. Architecture

- **Store:** dictionary + exact LRU list + TTL index under one lock. Every effective write gets a **sequence number** and is emitted as a normalized entry (absolute expiry timestamps, full values): `SET key value [PXAT ms]`, `DEL`, `PEXPIREAT`, `PERSIST`, `FLUSHALL`. Expiry and LRU eviction are local and not logged.
- **WAL (MSMQ):** one writer thread batches entries into one MSMQ transaction (group commit) on a transactional segment queue `<WalQueue>_00000001`. Checkpoint = rotate to a new segment, write a snapshot (records the sequence), delete the older segments. Startup = load snapshot, replay all segments, skip entries at or below the snapshot sequence.
- **Replication (MSMQ):** the primary sends batches to each replica's transactional queue (remote queues work through DIRECT format names; MSMQ's outgoing queue keeps messages while the replica is down). The replica applies entries idempotently by sequence number. A **gap** (missed entries) or a fresh replica triggers a `hello` to the primary's control queue; the primary answers with a chunked **snapshot** (begin / chunks / end) followed by the live stream.
- **Pub/Sub:** in-process hub with bounded per-subscriber buffers (a subscriber that cannot keep up is disconnected, like Redis). `PUBLISH` also goes to replicas.
- **Transports:** TCP/RESP (primary), optional MSMQ request/reply. One host: console, Windows Service or systemd (TCP only on Linux).

## 3. Durability modes (WAL)

| WalMode | Reply is sent | Loses on a crash |
|---|---|---|
| `async` (default) | immediately; the log is written in the background | the last few milliseconds of writes |
| `sync` | after the MSMQ transaction commits (`WalSyncTimeoutMs`, default 5000) | nothing acknowledged |

- Sync mode on timeout returns an error but the change **is already applied in memory** (it may become durable later).
- Writes are rejected with `BUSY` when more than `WalMaxBacklog` entries are waiting to be logged.
- A corrupt log message stops startup (like Redis AOF). `WalSkipCorrupt=true` skips it with an error log.
- Requires `PersistenceEnabled=true` (snapshots truncate the log) and `MaxValueLength <= 250000` (MSMQ messages are limited to 4 MB).

## 4. Replication

| Setting | Where | Meaning |
|---|---|---|
| `Role` | both | `primary` (default) or `replica` |
| `Replicas` | primary | destination queues, e.g. `FormatName:DIRECT=OS:host2\private$\msmqcache_repl` |
| `ControlQueue` | primary | receives resync requests |
| `ReplicationQueue` | replica | local transactional queue it consumes |
| `ReplicaAdvertisedQueue` | replica | how the primary reaches that queue; **must equal** the primary's `Replicas` entry |
| `PrimaryControlQueue` | replica | e.g. `FormatName:DIRECT=OS:primary\private$\msmqcache_ctl` |

- Replicas are **read-only**: writes get `READONLY`. `INFO` reports `role:slave`.
- The primary only answers resync requests from queues listed in `Replicas`.
- If a replica is unreachable so long that the backlog exceeds `ReplicationMaxBacklog`, the backlog is dropped and the replica full-resyncs when it returns.
- Replication is asynchronous: a primary crash can lose entries that were not yet sent. There is no automatic failover and no `WAIT`.
- Setup order: start the primary first (it creates the control queue), then the replicas.

## 4.1 MSMQ features used

| MSMQ feature | Used for |
|---|---|
| Transactional queues + `MessageQueueTransaction` | atomic, ordered WAL batches; exactly-once replication |
| Recoverable (disk-backed) messages | WAL and replication survive restarts |
| Store-and-forward (outgoing queues) | replication while the replica or network is down |
| `TimeToReachQueue` | `ReplicationTimeToReachQueueSeconds` (0 = keep trying) |
| Dead-letter queue | `ReplicationUseDeadLetterQueue` (undeliverable replication messages; purge it) |
| Sender journal | `ReplicationJournal` (audit copy of every message; counts toward quota) |
| Acknowledgments + administration queue | `ReplicationAcknowledge`: negative acks are logged and counted (`replication_nacks`) |
| Priority | resync / hello / snapshot messages are sent at higher priority |
| Authentication / encryption / sender ID | `ReplicationAuthenticate`, `ReplicationEncrypt`, `RequireAuthenticatedReplication` |
| Queue quotas | `WalQueueMaxSizeKb`, `QueueMaxSizeKb` |
| Queue ACLs | `ApplyAclOnStartup` + `QueueAcl` for the request queue |
| Direct / remote format names | replicas on other machines |

Not used: multicast (non-transactional), triggers, clustering, public queues / AD routing, HTTP queues (HTTP/TCP format names work as destinations but are untested).

## 5. Redis compatibility

RESP2 over TCP, pipelining, inline commands, `AUTH`, optional TLS. Strings only.

Commands: `PING ECHO GET SET (EX PX EXAT PXAT NX XX) SETNX SETEX PSETEX MGET MSET GETDEL APPEND STRLEN DEL UNLINK EXISTS INCR DECR INCRBY DECRBY EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST TYPE KEYS DBSIZE FLUSHDB FLUSHALL SAVE INFO CONFIG AUTH SELECT(0) CLIENT COMMAND PUBLISH SUBSCRIBE UNSUBSCRIBE QUIT`.

Not supported: lists, hashes, sets, sorted sets, streams, `PSUBSCRIBE`, transactions, Lua, RESP3/`HELLO`, `SCAN`, cluster, ACL users. `KEYS` is O(n), capped at 10,000 keys. `INFO` has no `redis_version`.

### Binary data, pickle and other serializers

Values are **UTF-8 text**. A `SET` with invalid UTF-8 is rejected with `-ERR Protocol error` and the connection is closed (never stored corrupted).

| Value format | Works today? |
|---|---|
| Plain text, JSON | Yes |
| Base64 / hex of any bytes (pickle, msgpack, protobuf, gzip, images) | Yes (+33% size) |
| Raw pickle (protocol 1+), msgpack, protobuf, gzip, images | No |

```python
import base64, pickle, redis
r = redis.Redis(host="127.0.0.1", port=6379)   # add password=... if set
def put(key, obj, ex=None):
    r.set(key, base64.b64encode(pickle.dumps(obj, protocol=5)).decode("ascii"), ex=ex)
def get(key):
    v = r.get(key)
    return None if v is None else pickle.loads(base64.b64decode(v))
```

`pickle.loads` runs arbitrary code: only unpickle data from a cache nobody untrusted can write to. Planned: binary-safe values.

## 6. Configuration

Config file lookup (never the current directory): `--config <path>`, env `MSMQCACHE_CONFIG`, `%ProgramData%\MsmqCache\config.json`, `<exe dir>\config.json` (created with defaults). Relative `DataDirectory` / `TlsCertificatePath` resolve against the exe folder. Samples in `config/`: default, `wal`, `primary`, `replica`, `secure`.

Runtime `CONFIG SET` (persisted): `maxmemorymb maxkeys evictionpolicy defaultttlseconds snapshotintervalseconds persistenceenabled password maxclients idletimeoutseconds replyrecoverable usedeadletterqueue`. Everything else needs a restart. `CONFIG GET` hides passwords.

Key settings: `BindAddress` (127.0.0.1), `Port` (6379), `Password`, `TlsEnabled`, `MaxMemoryMb` (256), `EvictionPolicy` (allkeys-lru | noeviction), `DisabledCommands`, `EnablePubSub`, `EnableMsmq` (request/reply door, off), `WalEnabled` (off), `Role` (primary).

## 7. Security checklist

- Keep `BindAddress` on loopback unless remote access is required; then set a long `Password` **and** TLS.
- Secure the MSMQ side: queue ACLs (`QueueAcl`), `ReplicationAuthenticate` / `ReplicationEncrypt` / `RequireAuthenticatedReplication` (need a domain or internal certificates), and restrict who can send to `msmqcache_ctl` and the replication queues: anyone who can write there can inject writes or force resyncs.
- Restrict the config file (clear-text password) and the data directory (snapshots hold all values).
- Disable commands you do not need: `["FLUSHALL","FLUSHDB","CONFIG","KEYS"]`.
- Dead-letter and journal queues count against the machine MSMQ quota: monitor and purge them.

## 8. Operations

- **Windows:** `scripts\install-service.ps1 -Dir <folder> [-EnableMsmq] [-OpenFirewall]` (enable the MSMQ feature when WAL/replication is used). **Linux:** `scripts/msmqcached.service` (TCP only; WAL/replication need Windows).
- **Monitoring (`INFO`):** `write_seq`, `wal_backlog`, `wal_durable_seq`, `wal_segment`, `wal_errors`, `replication_backlog`, `replication_nacks`, `connected_clients`, `pubsub_*`, memory, hits/misses.
- **Backups:** copy `data\cache.snapshot.json` (atomic) together with the WAL segment queues' content if you need point-in-time recovery.
- **Verify:** `python scripts/smoke_test.py`, `redis-cli PING`, `msmqcache INFO`.
- **MSMQ health:** the December 2025 Windows update (KB5071546) broke MSMQ sends on some Windows 10 / Server 2012-2019 hosts (storage folder permissions): test patched hosts. `Experimental.System.Messaging` is a community port, not an official client.

## 9. Testing and CI

- Unit + handler + **real TCP** tests (RESP framing, pipelining, auth, pub/sub): run on Windows and Ubuntu in `ci`.
- MSMQ tests (`MsmqCache.MsmqTests`, gated by `MSMQCACHE_MSMQ_TESTS=1`): request transport, WAL replay from the log alone, checkpoint/segment rotation, size-triggered checkpoint, replication full sync, live stream, read-only replica, pub/sub forwarding, gap recovery. Run by `integration-msmq` (manual / weekly, not a release gate).
- `build` packages win-x64 and linux-x64; `release` publishes GitHub releases from `vX.Y.Z` tags.

## 10. Licensing

- **msmqcache:** MIT.
- **MSMQ:** a Windows component, no license your code inherits; governed by your Windows license.
- **Experimental.System.Messaging 1.2.0:** Ms-RL (file-level copyleft; your files can stay MIT). Notices and the full Ms-RL text ship in `THIRD-PARTY-NOTICES.md` inside every release archive.
- **.NET libraries/runtime:** MIT. Not legal advice.

## 11. Roadmap

Binary-safe values, hashes/lists/sets/`SCAN`, `PSUBSCRIBE`, RESP3, ACL users, automatic failover / `WAIT`, Prometheus metrics, MSMQ performance counters.
