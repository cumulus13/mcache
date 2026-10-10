// File: src/MsmqCache.Client/Program.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: CLI (msmqcache): TCP/RESP by default, optional MSMQ transport; one-shot and REPL
// License: MIT

using System.Text;
using System.Text.Json;
using MsmqCache;

// Settings precedence: command-line > env (MSMQCACHE_PASSWORD) > client.json > defaults.
// client.json lookup: env MSMQCACHE_CLIENT_CONFIG | %ProgramData%\MsmqCache\client.json | <exe dir>\client.json
var cc = LoadConfig();
var rest = new List<string>();
for (int i = 0; i < args.Length; i++)
{
    string a = args[i];
    bool more = i + 1 < args.Length;
    if ((a == "-h" || a == "--host") && more) cc.Host = args[++i];
    else if ((a == "-p" || a == "--port") && more && int.TryParse(args[i + 1], out var port)) { cc.Port = port; i++; }
    else if ((a == "-a" || a == "--password") && more) cc.Password = args[++i];
    else if (a == "--tls") cc.Tls = true;
    else if (a == "--insecure") cc.TlsAllowUntrusted = true;
    else if (a == "--msmq" && more) { cc.Transport = "msmq"; cc.MsmqQueue = args[++i]; }
    else if (a == "--timeout" && more && int.TryParse(args[i + 1], out var t) && t > 0) { cc.TimeoutSeconds = t; i++; }
    else if (a == "--help") { Usage(); return 0; }
    else { rest.AddRange(args.Skip(i)); break; }
}
var envPw = Environment.GetEnvironmentVariable("MSMQCACHE_PASSWORD");
if (string.IsNullOrEmpty(cc.Password) && !string.IsNullOrEmpty(envPw)) cc.Password = envPw;

try
{
    var timeout = TimeSpan.FromSeconds(cc.TimeoutSeconds);
    ICacheClient client = cc.Transport.Equals("msmq", StringComparison.OrdinalIgnoreCase)
        ? new MsmqCacheClient(new MsmqClientOptions
        {
            RequestQueue = cc.MsmqQueue, Timeout = timeout,
            Recoverable = cc.Recoverable, UseAuthentication = cc.UseAuthentication, UseEncryption = cc.UseEncryption
        })
        : new TcpCacheClient(new TcpClientOptions
        {
            Host = cc.Host, Port = cc.Port, Password = cc.Password, Tls = cc.Tls,
            TlsAllowUntrusted = cc.TlsAllowUntrusted, ConnectTimeout = timeout, CommandTimeout = timeout
        });
    using (client)
    {
        string target = cc.Transport.Equals("msmq", StringComparison.OrdinalIgnoreCase) ? cc.MsmqQueue : $"{cc.Host}:{cc.Port}";

        if (rest.Count > 0)   // one-shot: msmqcache SET foo bar EX 60
            return Print(client.Execute(rest[0], rest.Skip(1).ToArray()));

        Console.WriteLine($"=== MCACHE CLI ({target}) ===  type 'exit' to quit\n");
        while (true)
        {
            Console.Write(target + "> ");
            var line = Console.ReadLine();
            if (line == null || line.Trim().Equals("exit", StringComparison.OrdinalIgnoreCase) ||
                line.Trim().Equals("quit", StringComparison.OrdinalIgnoreCase)) break;
            if (string.IsNullOrWhiteSpace(line)) continue;
            try
            {
                var tok = Tokenize(line);
                Print(client.Execute(tok[0], tok.Skip(1).ToArray()));
            }
            catch (TimeoutException ex) { Console.WriteLine("(timeout) " + ex.Message); }
            catch (CacheClientException ex) { Console.WriteLine("(error) " + ex.Message); }
            catch (FormatException ex) { Console.WriteLine("(error) " + ex.Message); }
        }
    }
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine("Error: " + ex.Message);
    return 1;
}

static void Usage() => Console.WriteLine("""
    msmqcache [options] [COMMAND args...]
      -h, --host <host>        default 127.0.0.1
      -p, --port <port>        default 6379
      -a, --password <pw>      or env MSMQCACHE_PASSWORD
          --tls                use TLS
          --insecure           accept self-signed server certificate
          --msmq <queue>       use the MSMQ transport instead of TCP
          --timeout <seconds>  default 5
    Without COMMAND an interactive prompt starts.
    """);

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
    public string Transport { get; set; } = "tcp";          // tcp | msmq
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 6379;
    public string Password { get; set; } = "";
    public bool Tls { get; set; } = false;
    public bool TlsAllowUntrusted { get; set; } = false;
    public int TimeoutSeconds { get; set; } = 5;
    // MSMQ transport only
    public string MsmqQueue { get; set; } = @".\Private$\MsmqCacheRequests";
    public bool Recoverable { get; set; } = false;
    public bool UseAuthentication { get; set; } = false;
    public bool UseEncryption { get; set; } = false;
}
