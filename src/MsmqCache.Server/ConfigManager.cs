// File: src/MsmqCache.Server/ConfigManager.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Server config: resolved from exe dir / ProgramData (never the current dir), runtime get/set
// License: MIT

using System.Globalization;
using System.Text.Json;

namespace MsmqCache;

public sealed class ServerConfig
{
    public string RequestQueue { get; set; } = @".\Private$\MsmqCacheRequests";
    public string DataDirectory { get; set; } = "data";          // relative => resolved against exe dir
    public bool PersistenceEnabled { get; set; } = true;
    public int SnapshotIntervalSeconds { get; set; } = 60;
    public long MaxMemoryMb { get; set; } = 256;                 // 0 = unlimited
    public long MaxKeys { get; set; } = 0;                       // 0 = unlimited
    public string EvictionPolicy { get; set; } = "allkeys-lru";  // or "noeviction"
    public long DefaultTtlSeconds { get; set; } = 0;             // 0 = keys never expire
    public int WorkerThreads { get; set; } = 4;
    public int ReplyTtlSeconds { get; set; } = 30;
    public int MaxKeyLength { get; set; } = 512;
    public int MaxValueLength { get; set; } = 1_000_000;
    public int SweepIntervalMs { get; set; } = 1000;
    public string[] DisabledCommands { get; set; } = Array.Empty<string>(); // e.g. ["FLUSHALL","CONFIG","KEYS"]
}

public sealed class ConfigManager
{
    static readonly JsonSerializerOptions J = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true
    };

    readonly object _lock = new();
    public static string BaseDir => AppContext.BaseDirectory;   // exe folder, NOT current dir
    public string FilePath { get; }
    public ServerConfig Current { get; }

    ConfigManager(string path, ServerConfig cfg) { FilePath = path; Current = cfg; }

    public string DataDir => Path.GetFullPath(Environment.ExpandEnvironmentVariables(Current.DataDirectory), BaseDir);
    public string SnapshotPath => Path.Combine(DataDir, "cache.snapshot.json");

    // Order: --config <path> | env MSMQCACHE_CONFIG | %ProgramData%\MsmqCache\config.json (if exists) | <exe dir>\config.json
    public static ConfigManager Load(string[] args)
    {
        string path = Resolve(args);
        ServerConfig cfg;
        if (File.Exists(path))
        {
            cfg = JsonSerializer.Deserialize<ServerConfig>(File.ReadAllText(path), J) ?? new ServerConfig();
        }
        else
        {
            cfg = new ServerConfig();
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, JsonSerializer.Serialize(cfg, J));
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { /* run with defaults */ }
        }
        Validate(cfg);
        return new ConfigManager(path, cfg);
    }

    static string Resolve(string[] args)
    {
        for (int i = 0; i < args.Length - 1; i++)
            if (args[i] == "--config")
                return Path.GetFullPath(Environment.ExpandEnvironmentVariables(args[i + 1]), BaseDir);

        var env = Environment.GetEnvironmentVariable("MSMQCACHE_CONFIG");
        if (!string.IsNullOrWhiteSpace(env))
            return Path.GetFullPath(Environment.ExpandEnvironmentVariables(env), BaseDir);

        var pd = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "MsmqCache", "config.json");
        if (File.Exists(pd)) return pd;

        return Path.Combine(BaseDir, "config.json");
    }

    static void Validate(ServerConfig c)
    {
        c.EvictionPolicy = (c.EvictionPolicy ?? "").ToLowerInvariant();
        c.DisabledCommands ??= Array.Empty<string>();
        if (string.IsNullOrWhiteSpace(c.RequestQueue)) throw new InvalidDataException("RequestQueue is empty");
        if (c.EvictionPolicy is not ("allkeys-lru" or "noeviction")) throw new InvalidDataException("EvictionPolicy must be allkeys-lru or noeviction");
        if (c.WorkerThreads is < 1 or > 64) throw new InvalidDataException("WorkerThreads must be 1..64");
        if (c.MaxMemoryMb < 0 || c.MaxKeys < 0 || c.DefaultTtlSeconds < 0) throw new InvalidDataException("negative limit");
        if (c.MaxKeyLength < 1 || c.MaxValueLength < 1 || c.ReplyTtlSeconds < 1) throw new InvalidDataException("invalid size/ttl limit");
    }

    public IEnumerable<KeyValuePair<string, string>> All()
    {
        foreach (var p in typeof(ServerConfig).GetProperties())
        {
            var v = p.GetValue(Current);
            string s = v is string[] arr ? string.Join(",", arr) : Convert.ToString(v, CultureInfo.InvariantCulture) ?? "";
            yield return new(p.Name.ToLowerInvariant(), s);
        }
    }

    // Runtime-settable subset. Returns error text or null on success. Persists to the config file.
    public string? TrySet(string name, string value)
    {
        lock (_lock)
        {
            var c = Current;
            switch (name.ToLowerInvariant())
            {
                case "maxmemorymb":
                    { if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var v)) return "invalid value"; c.MaxMemoryMb = v; break; }
                case "maxkeys":
                    { if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var v)) return "invalid value"; c.MaxKeys = v; break; }
                case "defaultttlseconds":
                    { if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var v)) return "invalid value"; c.DefaultTtlSeconds = v; break; }
                case "snapshotintervalseconds":
                    { if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var v)) return "invalid value"; c.SnapshotIntervalSeconds = v; break; }
                case "persistenceenabled":
                    { if (!bool.TryParse(value, out var v)) return "invalid value (true/false)"; c.PersistenceEnabled = v; break; }
                case "evictionpolicy":
                    {
                        var v = value.ToLowerInvariant();
                        if (v is not ("allkeys-lru" or "noeviction")) return "must be allkeys-lru or noeviction";
                        c.EvictionPolicy = v; break;
                    }
                default: return $"unknown or read-only setting '{name}' (restart-only settings: edit the file)";
            }
            try
            {
                var tmp = FilePath + ".tmp";
                File.WriteAllText(tmp, JsonSerializer.Serialize(c, J));
                File.Move(tmp, FilePath, true);
            }
            catch (Exception ex) { return "applied in memory, but config file not written: " + ex.Message; }
            return null;
        }
    }
}
