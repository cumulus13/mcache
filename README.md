# msmqcache

A **Redis-compatible cache server for .NET 8 that uses MSMQ as its durability and replication backend.**

- Clients use **TCP (default port 6379)** with `redis-cli` or any Redis library (RESP2). Strings only: TTL, LRU, memory limits, snapshots, `AUTH`, optional TLS, pipelining, pub/sub.
- **MSMQ is the backend**: write-ahead log (transactional queues, replayed at startup), replication to replicas (store-and-forward, exactly-once, full-resync fallback), pub/sub forwarding, plus an optional request/reply queue door for Windows clients.
- MSMQ is *not* the key/value store: a queue has no lookup by key, no in-place update and no per-key TTL.

> **Status:** syntax-checked and test-covered, but never compiled or run (the build sandbox had no .NET SDK, and MSMQ is Windows-only). Build, run the tests, and read [`docs/PRODUCTION_GUIDE.md`](docs/PRODUCTION_GUIDE.md) before relying on it.

- `msmqcached` - server (console, Windows Service, systemd on Linux for TCP-only)
- `msmqcache` - CLI (TCP by default, `--msmq <queue>` for the queue door)

## Quick start

```powershell
dotnet run --project src/MsmqCache.Server            # 127.0.0.1:6379, in-memory + snapshots
redis-cli -p 6379 SET hello world EX 60
redis-cli -p 6379 GET hello
python scripts/smoke_test.py --port 6379             # black-box RESP check (incl. pub/sub)
```

## Use MSMQ as the backend

Enable the MSMQ feature once (`Enable-WindowsOptionalFeature -Online -FeatureName MSMQ-Server -All`), then:

**Durable (write-ahead log)** - `config/config.wal.sample.json`

```json
{ "PersistenceEnabled": true, "WalEnabled": true, "WalMode": "sync", "MaxValueLength": 250000 }
```

`sync` replies after the MSMQ transaction commits; `async` replies immediately and logs in the background. Restart replays the log on top of the last snapshot.

**Replication** - `config/config.primary.sample.json` and `config/config.replica.sample.json`. Start the primary first, then the replicas. Replicas are read-only and full-sync automatically when they are new or miss entries. `ReplicaAdvertisedQueue` (replica) must equal the primary's `Replicas` entry.

**Pub/Sub** - `SUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH`; `PUBLISH` is forwarded to replicas over MSMQ.

## Security defaults

- Binds **127.0.0.1**. For remote access set `BindAddress` **and** `Password` (and TLS); the server warns otherwise.
- `Password` = Redis `requirepass`; wrong attempts are delayed and the connection is dropped after 5. `CONFIG GET` hides passwords.
- `DisabledCommands`, MSMQ queue ACLs (`QueueAcl`), `ReplicationAuthenticate/Encrypt` - see the guide. Restrict who can write to the control and replication queues.

## Binary data, pickle and other serializers

Values are **UTF-8 text**. A `SET` with invalid UTF-8 is rejected (`-ERR Protocol error`) and the connection is closed. Raw pickle (protocol 1+), msgpack, protobuf, gzip and images must be **base64-encoded** first:

```python
import base64, pickle, redis
r = redis.Redis(host="127.0.0.1", port=6379)
def put(key, obj, ex=None): r.set(key, base64.b64encode(pickle.dumps(obj, protocol=5)).decode("ascii"), ex=ex)
def get(key):
    v = r.get(key); return None if v is None else pickle.loads(base64.b64decode(v))
```

`pickle.loads` executes arbitrary code: only unpickle from a cache nobody untrusted can write to. Planned: binary-safe values.

## Commands

`PING ECHO GET SET(EX PX EXAT PXAT NX XX) SETNX SETEX PSETEX MGET MSET GETDEL APPEND STRLEN DEL UNLINK EXISTS INCR DECR INCRBY DECRBY EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST TYPE KEYS DBSIZE FLUSHDB FLUSHALL SAVE INFO CONFIG AUTH SELECT(0) CLIENT COMMAND PUBLISH SUBSCRIBE UNSUBSCRIBE QUIT`

Not supported: lists, hashes, sets, `PSUBSCRIBE`, transactions, Lua, RESP3, cluster, ACL users, automatic failover.

## Config (never read from the current directory)

`--config <path>` -> env `MSMQCACHE_CONFIG` -> `%ProgramData%\MsmqCache\config.json` -> `<exe dir>\config.json` (created with defaults). Relative `DataDirectory` / `TlsCertificatePath` resolve against the exe folder. Samples: `config/`. Runtime `CONFIG SET` (persisted): `maxmemorymb maxkeys evictionpolicy defaultttlseconds snapshotintervalseconds persistenceenabled password maxclients idletimeoutseconds replyrecoverable usedeadletterqueue`.

## Layout

```
.github/workflows/  ci  build  release  integration-msmq
src/MsmqCache.Protocol  request/response types
src/MsmqCache.Server    store, RESP/TCP, pub/sub, MSMQ WAL + replication, MSMQ request transport, host
src/MsmqCache.Client    msmqcache CLI, TcpCacheClient, MsmqCacheClient
tests/MsmqCache.Tests       unit, handler, WAL-log semantics, real-TCP tests (run everywhere)
tests/MsmqCache.MsmqTests   WAL / replication / queue tests (MSMQCACHE_MSMQ_TESTS=1, Windows + MSMQ)
config/  scripts/  docs/PRODUCTION_GUIDE.md  THIRD-PARTY-NOTICES.md  LICENSE
```

## Build / test / package

```powershell
dotnet build msmqcache.sln -c Release
dotnet test  msmqcache.sln -c Release                       # MSMQ tests are skipped by default
$env:MSMQCACHE_MSMQ_TESTS = "1"; dotnet test tests/MsmqCache.MsmqTests -c Release
.\scripts\package.ps1 -Version 1.0.0                        # win-x64 zip
.\scripts\package.ps1 -Version 1.0.0 -Rid linux-x64 -SelfContained
```

Windows service: `.\scripts\install-service.ps1 -Dir C:\msmqcache\server [-EnableMsmq] [-OpenFirewall]`. Linux: `scripts/msmqcached.service`.

## CI / release

`ci` (build + test on Windows and Ubuntu) | `build` (push to main: package win-x64 + linux-x64) | `release` (tag `vX.Y.Z`; `-rc` = prerelease) | `integration-msmq` (manual / weekly: enables MSMQ on a Windows runner and runs the MSMQ tests; not a release gate).

## License

MIT. Dependency notices (Ms-RL for `Experimental.System.Messaging`, MIT for .NET) are in `THIRD-PARTY-NOTICES.md`, shipped in every release archive.


## 👤 Author
        
[Hadi Cahyadi](mailto:cumulus13@gmail.com)
    

[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/cumulus13)

[![Donate via Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cumulus13)
 
[Support me on Patreon](https://www.patreon.com/cumulus13)