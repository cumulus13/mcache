# MsmqCache (mcached)

A **Redis-compatible cache server**: it listens on a **TCP port (default 6379)** and speaks **RESP2**, so `redis-cli` and
ordinary Redis client libraries can talk to it. Strings only (no lists/hashes/sets yet). Optional **MSMQ** transport
for Windows-only setups.

- `mcached` - server (console, Windows Service, or systemd on Linux)
- `mcache` - CLI client (TCP by default, `--msmq <queue>` for MSMQ)

Features: TTL, exact LRU eviction, memory / key limits, `AUTH` password, optional TLS, pipelining, idle timeout,
max clients, atomic snapshots (persistence), runtime `CONFIG GET/SET`, disabled-command list.

> Not a full Redis: no replication, clustering, pub/sub, transactions, Lua, ACL users, RESP3, or data types other than strings.
> Keys and values are **UTF-8 text** (invalid UTF-8 is rejected, not silently altered). Single store lock; fast for a cache, not a Redis replacement for heavy workloads.

## Layout

```
.github/workflows/   ci.yml  build.yml  release.yml  integration-msmq.yml
src/MsmqCache.Protocol   request/response types (used by MSMQ transport + clients)
src/MsmqCache.Server     mcached: store, RESP/TCP transport, MSMQ transport, host
src/MsmqCache.Client     mcache CLI + TcpCacheClient + MsmqCacheClient
tests/MsmqCache.Tests    unit tests + real-TCP integration tests (run everywhere)
tests/MsmqCache.MsmqTests  MSMQ transport tests (skipped unless MSMQCACHE_MSMQ_TESTS=1)
config/  scripts/  THIRD-PARTY-NOTICES.md  LICENSE
```

## Quick start

```bash
dotnet run --project src/MsmqCache.Server            # listens on 127.0.0.1:6379
redis-cli -p 6379 SET hello "world" EX 60            # any Redis client works
redis-cli -p 6379 GET hello
dotnet run --project src/MsmqCache.Client -- GET hello   # bundled CLI
python scripts/smoke_test.py --port 6379             # black-box RESP check of a running server
```

## Security defaults

- Binds **127.0.0.1** only. To accept remote clients set `BindAddress` (e.g. `0.0.0.0`) **and** `Password`; the server logs a warning if you don't.
- `Password` = Redis `requirepass` (`AUTH <pw>`); wrong attempts are delayed and the connection is dropped after 5.
- `TlsEnabled` + `TlsCertificatePath` (.pfx) turns on TLS 1.2/1.3. Use `mcache --tls [--insecure]`.
- `DisabledCommands` e.g. `["FLUSHALL","FLUSHDB","CONFIG","KEYS"]`.
- The config file holds the password in clear text: restrict its file ACL.
- `CONFIG GET` never reveals `password` / `tlscertificatepassword`.

## Config (never read from the current directory)

Lookup order: `--config <path>` -> env `MSMQCACHE_CONFIG` -> `%ProgramData%\MsmqCache\config.json` (if present) -> `<exe dir>\config.json`
(created with defaults if missing). `DataDirectory` and `TlsCertificatePath` are relative to the **exe folder** and expand `%ENVVARS%`.
Samples: `config/config.sample.json`, `config/config.secure.sample.json`, `config/client.sample.json`.

Runtime (persisted to the file): `CONFIG SET maxmemorymb|maxkeys|evictionpolicy|defaultttlseconds|snapshotintervalseconds|persistenceenabled|password|maxclients|idletimeoutseconds|replyrecoverable|usedeadletterqueue <value>`.
Other keys need a restart. `CONFIG GET *` lists all.

## Commands

`PING ECHO GET SET(EX|PX|NX|XX) SETNX SETEX PSETEX MGET MSET GETDEL APPEND STRLEN DEL UNLINK EXISTS INCR DECR INCRBY DECRBY EXPIRE PEXPIRE TTL PTTL PERSIST TYPE KEYS DBSIZE FLUSHDB FLUSHALL SAVE INFO CONFIG AUTH SELECT(0 only) CLIENT(SETNAME/GETNAME/ID/SETINFO) COMMAND QUIT`

`KEYS` is O(n) and returns at most 10,000 keys. Some clients expect `redis_version` in `INFO`; it is intentionally not faked.

## MSMQ transport (optional, Windows)

Set `EnableMsmq: true`. Options: `RequestQueue`, `WorkerThreads`, `ReplyRecoverable`, `UseDeadLetterQueue`, `QueueMaxSizeKb`,
`QueueJournal`/`JournalMaxSizeKb`, `QueueAuthenticate`, `QueueEncryption` (none|optional|body), `RequireAuthenticatedRequests`,
`ApplyAclOnStartup` + `QueueAcl` (send|receive|full). Dead-letter and journal messages count against the machine quota: purge them.
Authentication/encryption need a domain (AD) or internal certificates. Messages are limited to 4 MB. Local machine is the tested design.

## Build / test / package

```powershell
dotnet build MsmqCache.sln -c Release
dotnet test  MsmqCache.sln -c Release          # includes real TCP tests
.\scripts\package.ps1 -Version 1.0.0           # win-x64 zip
.\scripts\package.ps1 -Version 1.0.0 -Rid linux-x64 -SelfContained   # tar.gz
```

Windows service: `.\scripts\install-service.ps1 -Dir C:\MsmqCache\server [-OpenFirewall] [-EnableMsmq]`.
Linux: see `scripts/mcached.service` (systemd).

## CI / release

- `ci.yml` - restore, build, test on Windows + Ubuntu (PRs; reusable via `workflow_call`)
- `build.yml` - push to main: ci, then package win-x64 + linux-x64 (framework-dependent + self-contained)
- `release.yml` - tag `vX.Y.Z` (`vX.Y.Z-rc.1` = prerelease): ci, package, `gh release create` with archives + `.sha256`
- `integration-msmq.yml` - manual / weekly: enables MSMQ on a Windows runner and runs the MSMQ tests (not a release gate)

## License

MIT for this project. Dependency notices (Ms-RL for `Experimental.System.Messaging`, MIT for .NET) are in `THIRD-PARTY-NOTICES.md`
and are shipped inside every release archive.

## 👤 Author
        
[Hadi Cahyadi](mailto:cumulus13@gmail.com)
    

[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/cumulus13)

[![Donate via Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cumulus13)
 
[Support me on Patreon](https://www.patreon.com/cumulus13)