# MsmqCache

Redis-like cache server over **MSMQ** for Windows / .NET 8. Request/reply over a private queue, TTL, exact LRU eviction,
memory/key limits, atomic snapshots, runtime `CONFIG GET/SET`, Windows Service support.

- `mcached.exe` - server (console or Windows Service)
- `mcache.exe` - CLI client (one-shot or REPL)

> Not Redis wire-compatible. Single store lock; throughput is bounded by MSMQ.

## Layout

```
.github/workflows/   ci.yml  build.yml  release.yml
src/Shared/          wire protocol (linked into server + client)
src/MsmqCache.Server server (mcached)
src/MsmqCache.Client client + CLI (mcache)
tests/               xUnit tests for the store (no MSMQ required)
config/              sample config.json / client.json
scripts/             package.ps1, install-service.ps1, uninstall-service.ps1
```

## Requirements

- Windows with the MSMQ feature: `Enable-WindowsOptionalFeature -Online -FeatureName MSMQ-Server -All`
- .NET 8 SDK to build; .NET 8 runtime to run the framework-dependent zip (self-contained zip needs nothing)

## Build / test / package

```powershell
dotnet build MsmqCache.sln -c Release
dotnet test  MsmqCache.sln -c Release
.\scripts\package.ps1 -Version 1.0.0            # artifacts\msmqcache-1.0.0-win-x64-framework-dependent.zip
.\scripts\package.ps1 -Version 1.0.0 -SelfContained
```

## Run

```powershell
.\server\mcached.exe                              # console
.\scripts\install-service.ps1 -Dir C:\MsmqCache\server   # Windows Service (admin)

.\client\mcache.exe SET user:1 "hello world" EX 60
.\client\mcache.exe GET user:1
.\client\mcache.exe INFO
.\client\mcache.exe                               # REPL
```

## Config (never read from the current directory)

Server lookup order:

1. `--config <path>`
2. env `MSMQCACHE_CONFIG`
3. `%ProgramData%\MsmqCache\config.json` (if it exists)
4. `<exe dir>\config.json` (created with defaults if missing)

`DataDirectory` is relative to the exe folder and supports `%ENVVARS%`. Client uses the same order with
`MSMQCACHE_CLIENT_CONFIG` / `client.json`; `--queue` and `--timeout` override.

Runtime changes (persisted to the file): `CONFIG SET maxmemorymb|maxkeys|evictionpolicy|defaultttlseconds|snapshotintervalseconds|persistenceenabled <value>`.
Other keys need a restart. `CONFIG GET *` lists everything.

## Commands

`PING GET SET(EX|PX|NX|XX) MGET MSET DEL EXISTS INCR DECR INCRBY DECRBY EXPIRE TTL PERSIST KEYS DBSIZE FLUSHALL SAVE INFO CONFIG`

## Notes

- **No auth in the protocol.** Access control = MSMQ queue ACL on `MsmqCacheRequests`. Use `DisabledCommands` to remove `FLUSHALL`, `CONFIG`, `KEYS`.
- Messages are non-recoverable (fast). Durability = periodic + shutdown snapshots; a hard crash loses up to `SnapshotIntervalSeconds`.
- MSMQ message cap is 4 MB; `KEYS` returns at most 10,000 keys.
- Clients create a private reply queue per instance and delete it on dispose; a crashed client leaves `MsmqCacheReply_*` behind.
- Local machine only is the supported/tested design.

## CI / release

- `ci.yml` - restore, build, test on PRs (also reusable via `workflow_call`)
- `build.yml` - on push to main: ci, then package (framework-dependent + self-contained) as workflow artifacts
- `release.yml` - on tag `vX.Y.Z` (or `vX.Y.Z-rc.1` for prerelease): ci, package, then `gh release create` with zips + `.sha256`

```
git tag v1.0.0 && git push origin v1.0.0
```

## License

MIT

---

## 👤 Author
        
[Hadi Cahyadi](mailto:cumulus13@gmail.com)
    

[![Buy Me a Coffee](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/cumulus13)

[![Donate via Ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/cumulus13)
 
[Support me on Patreon](https://www.patreon.com/cumulus13)