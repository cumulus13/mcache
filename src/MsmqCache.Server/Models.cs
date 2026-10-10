// File: src/MsmqCache.Server/Models.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Log entries, replication messages and the write-sink interface
// License: MIT

using System.Text.Json.Serialization;

namespace MsmqCache;

// One normalized, deterministic effect of a write (absolute expiry timestamps, full values).
public sealed class LogEntry
{
    [JsonPropertyName("s")] public long Seq { get; set; }
    [JsonPropertyName("c")] public string Cmd { get; set; } = "";
    [JsonPropertyName("a")] public string[] Args { get; set; } = Array.Empty<string>();

    // Worst-case JSON size (every char escaped as \uXXXX) used to keep MSMQ messages well under the 4 MB limit.
    public long EstimatedCost()
    {
        long n = 64 + Cmd.Length;
        foreach (var a in Args) n += a.Length + 8;
        return n * 6;
    }
}

public sealed class WalBatch
{
    [JsonPropertyName("e")] public List<LogEntry> Entries { get; set; } = new();
}

// Replication wire message. Types: "w" (writes), "pub" (pub/sub), "snap" (full sync), "hello" (replica -> primary).
public sealed class ReplMessage
{
    [JsonPropertyName("t")]   public string Type { get; set; } = "";
    [JsonPropertyName("e")]   public List<LogEntry>? Entries { get; set; }
    [JsonPropertyName("ch")]  public string? Channel { get; set; }
    [JsonPropertyName("m")]   public string? Payload { get; set; }
    [JsonPropertyName("seq")] public long Seq { get; set; }
    [JsonPropertyName("part")] public string? Part { get; set; }          // snap: begin | chunk | end
    [JsonPropertyName("i")]   public List<SnapEntry>? Items { get; set; }  // snap chunk
    [JsonPropertyName("q")]   public string? ReplicaQueue { get; set; }    // hello: queue the primary should use
}

public interface IWriteSink
{
    // Called under the store lock: must be cheap and must not block.
    void Append(LogEntry entry, bool replicate);
}

public enum ApplyResult { Applied, Duplicate, Gap }
