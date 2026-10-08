// File: src/MsmqCache.Client/Program.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: CLI: one-shot and interactive REPL (mcache)
// License: MIT

using System.Text;
using System.Text.Json;
using MsmqCache;

// Client config: --queue / --timeout args > env MSMQCACHE_CLIENT_CONFIG > %ProgramData%\MsmqCache\client.json > <exe dir>\client.json
var cc = LoadConfig();
var rest = new List<string>();
for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--queue" && i + 1 < args.Length) cc.RequestQueue = args[++i];
    else if (args[i] == "--timeout" && i + 1 < args.Length && int.TryParse(args[i + 1], out var t) && t > 0) { cc.TimeoutSeconds = t; i++; }
    else rest.Add(args[i]);
}

try
{
    using var client = new CacheClient(cc.RequestQueue, TimeSpan.FromSeconds(cc.TimeoutSeconds));

    if (rest.Count > 0)   // one-shot: mcache SET foo bar EX 60
        return Print(client.Execute(rest[0], rest.Skip(1).ToArray()));

    Console.WriteLine($"=== MSMQ CACHE CLI ({cc.RequestQueue}) ===  type 'exit' to quit\n");
    while (true)
    {
        Console.Write("cache> ");
        var line = Console.ReadLine();
        if (line == null || line.Trim().Equals("exit", StringComparison.OrdinalIgnoreCase)) break;
        if (string.IsNullOrWhiteSpace(line)) continue;
        try
        {
            var tok = Tokenize(line);
            Print(client.Execute(tok[0], tok.Skip(1).ToArray()));
        }
        catch (TimeoutException ex) { Console.WriteLine("(timeout) " + ex.Message); }
        catch (FormatException ex) { Console.WriteLine("(error) " + ex.Message); }
    }
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine("Error: " + ex.Message);
    return 1;
}

static int Print(Response r)
{
    switch (r.Kind)
    {
        case Kinds.Ok: Console.WriteLine(r.Str); return 0;
        case Kinds.Num: Console.WriteLine($"(integer) {r.Num}"); return 0;
        case Kinds.Nil: Console.WriteLine("(nil)"); return 0;
        case Kinds.Bulk:
            Console.WriteLine(r.Str != null && r.Str.Contains('\n') ? r.Str : "\"" + r.Str + "\"");
            return 0;
        case Kinds.List:
            if (r.Arr == null || r.Arr.Length == 0) Console.WriteLine("(empty)");
            else for (int i = 0; i < r.Arr.Length; i++)
                Console.WriteLine($"{i + 1}) " + (r.Arr[i] is null ? "(nil)" : "\"" + r.Arr[i] + "\""));
            return 0;
        default: Console.WriteLine("(error) " + r.Str); return 1;
    }
}

static List<string> Tokenize(string line)
{
    var t = new List<string>(); var sb = new StringBuilder(); bool q = false, any = false;
    for (int i = 0; i < line.Length; i++)
    {
        char c = line[i];
        if (q)
        {
            if (c == '\\' && i + 1 < line.Length) sb.Append(line[++i]);
            else if (c == '"') q = false;
            else sb.Append(c);
        }
        else if (c == '"') { q = true; any = true; }
        else if (char.IsWhiteSpace(c)) { if (sb.Length > 0 || any) { t.Add(sb.ToString()); sb.Clear(); any = false; } }
        else sb.Append(c);
    }
    if (q) throw new FormatException("unbalanced quotes");
    if (sb.Length > 0 || any) t.Add(sb.ToString());
    return t;
}

static ClientConfig LoadConfig()
{
    var opt = new JsonSerializerOptions { WriteIndented = true, PropertyNameCaseInsensitive = true, ReadCommentHandling = JsonCommentHandling.Skip };
    var env = Environment.GetEnvironmentVariable("MSMQCACHE_CLIENT_CONFIG");
    var pd = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "MsmqCache", "client.json");
    var path = !string.IsNullOrWhiteSpace(env) ? Path.GetFullPath(env, AppContext.BaseDirectory)
             : File.Exists(pd) ? pd
             : Path.Combine(AppContext.BaseDirectory, "client.json");
    if (File.Exists(path))
        return JsonSerializer.Deserialize<ClientConfig>(File.ReadAllText(path), opt) ?? new ClientConfig();
    var def = new ClientConfig();
    try { File.WriteAllText(path, JsonSerializer.Serialize(def, opt)); } catch { }
    return def;
}

sealed class ClientConfig
{
    public string RequestQueue { get; set; } = @".\Private$\MsmqCacheRequests";
    public int TimeoutSeconds { get; set; } = 5;
}
