// File: src/MsmqCache.Protocol/Protocol.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Wire protocol shared by server and client (JSON over MSMQ string bodies)
// License: MIT

using System.Text.Json;
using System.Text.Json.Serialization;

namespace MsmqCache;

public static class Kinds
{
    public const string Ok = "ok", Num = "num", Bulk = "bulk", Nil = "nil", List = "list", Err = "err";
}

public sealed class Request
{
    [JsonPropertyName("id")]   public string Id { get; set; } = "";
    [JsonPropertyName("cmd")]  public string Cmd { get; set; } = "";
    [JsonPropertyName("args")] public string[] Args { get; set; } = Array.Empty<string>();
}

public sealed class Response
{
    [JsonPropertyName("id")]   public string Id { get; set; } = "";
    [JsonPropertyName("kind")] public string Kind { get; set; } = Kinds.Nil;
    [JsonPropertyName("str")]  public string? Str { get; set; }
    [JsonPropertyName("num")]  public long Num { get; set; }
    [JsonPropertyName("arr")]  public string?[]? Arr { get; set; }

    public static Response Ok(string s = "OK") => new() { Kind = Kinds.Ok, Str = s };
    public static Response Number(long n)     => new() { Kind = Kinds.Num, Num = n };
    public static Response Bulk(string s)      => new() { Kind = Kinds.Bulk, Str = s };
    public static Response Nil()               => new();
    public static Response List(string?[] a)   => new() { Kind = Kinds.List, Arr = a };
    public static Response Error(string m)     => new() { Kind = Kinds.Err, Str = m };
}

public static class Wire
{
    static readonly JsonSerializerOptions Opt = new() { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };
    public static string Pack<T>(T v) => JsonSerializer.Serialize(v, Opt);
    public static T? Unpack<T>(string s) => JsonSerializer.Deserialize<T>(s, Opt);
}
