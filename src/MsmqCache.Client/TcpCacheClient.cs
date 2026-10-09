// File: src/MsmqCache.Client/TcpCacheClient.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: RESP2 TCP client (works with this server and with real Redis for simple commands)
// License: MIT

using System.Globalization;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;

namespace MsmqCache;

public sealed class TcpClientOptions
{
    public string Host { get; set; } = "127.0.0.1";
    public int Port { get; set; } = 6379;
    public string Password { get; set; } = "";
    public bool Tls { get; set; }
    public bool TlsAllowUntrusted { get; set; }            // accept self-signed server certificates
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromSeconds(5);
}

// Synchronous, thread-safe (one command at a time). Reconnects lazily after a failure.
// A command that fails mid-flight is NOT retried (it may have been applied): the exception is thrown.
public sealed class TcpCacheClient : ICacheClient
{
    readonly TcpClientOptions _o;
    readonly object _lock = new();
    TcpClient? _tcp;
    Stream? _s;
    readonly byte[] _buf = new byte[16 * 1024];
    int _pos, _len;

    public TcpCacheClient(TcpClientOptions options) => _o = options;

    public Response Execute(string cmd, params string[] args)
    {
        lock (_lock)
        {
            try
            {
                EnsureConnected();
                Send(cmd, args);
                return ReadReply();
            }
            catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException
                                          or System.Security.Authentication.AuthenticationException)
            {
                Disconnect();
                bool timeout = ex is IOException { InnerException: SocketException { SocketErrorCode: SocketError.TimedOut } }
                               || ex is SocketException { SocketErrorCode: SocketError.TimedOut };
                if (timeout) throw new TimeoutException($"No reply to {cmd} within {_o.CommandTimeout.TotalSeconds}s");
                throw new CacheClientException("connection error: " + ex.Message);
            }
        }
    }

    void EnsureConnected()
    {
        if (_tcp is { Connected: true } && _s != null) return;
        Disconnect();

        var tcp = new TcpClient { NoDelay = true };
        try
        {
            var connect = tcp.ConnectAsync(_o.Host, _o.Port);
            if (!connect.Wait(_o.ConnectTimeout)) throw new TimeoutException($"Connect to {_o.Host}:{_o.Port} timed out");
            connect.GetAwaiter().GetResult();
            tcp.ReceiveTimeout = tcp.SendTimeout = (int)_o.CommandTimeout.TotalMilliseconds;

            Stream s = tcp.GetStream();
            if (_o.Tls)
            {
                var ssl = new SslStream(s, false, (_, _, _, errors) => _o.TlsAllowUntrusted || errors == SslPolicyErrors.None);
                ssl.AuthenticateAsClient(_o.Host);
                s = ssl;
            }
            _tcp = tcp; _s = s; _pos = _len = 0;

            if (_o.Password.Length > 0)
            {
                Send("AUTH", new[] { _o.Password });
                var r = ReadReply();
                if (r.Kind == Kinds.Err) { Disconnect(); throw new CacheClientException("AUTH failed: " + r.Str); }
            }
        }
        catch
        {
            if (_tcp == null) tcp.Dispose();
            else Disconnect();
            throw;
        }
    }

    void Disconnect()
    {
        try { _s?.Dispose(); } catch { }
        try { _tcp?.Dispose(); } catch { }
        _s = null; _tcp = null; _pos = _len = 0;
    }

    void Send(string cmd, string[] args)
    {
        var ms = new MemoryStream();
        void Bulk(string s)
        {
            var b = Encoding.UTF8.GetBytes(s);
            ms.Write(Encoding.ASCII.GetBytes("$" + b.Length.ToString(CultureInfo.InvariantCulture) + "\r\n"));
            ms.Write(b);
            ms.Write("\r\n"u8);
        }
        ms.Write(Encoding.ASCII.GetBytes("*" + (args.Length + 1).ToString(CultureInfo.InvariantCulture) + "\r\n"));
        Bulk(cmd);
        foreach (var a in args) Bulk(a);
        _s!.Write(ms.GetBuffer(), 0, (int)ms.Length);
        _s.Flush();
    }

    int ReadByte()
    {
        if (_pos >= _len)
        {
            _len = _s!.Read(_buf, 0, _buf.Length);
            _pos = 0;
            if (_len <= 0) { _len = 0; throw new IOException("connection closed by server"); }
        }
        return _buf[_pos++];
    }

    string ReadLine()
    {
        var sb = new StringBuilder();
        while (true)
        {
            int b = ReadByte();
            if (b == '\r') { ReadByte(); return sb.ToString(); }   // consume '\n'
            sb.Append((char)b);
            if (sb.Length > 1024 * 1024) throw new IOException("line too long");
        }
    }

    byte[] ReadBytes(int n)
    {
        var res = new byte[n];
        for (int i = 0; i < n; i++) res[i] = (byte)ReadByte();
        return res;
    }

    string? ReadBulkBody(int len)
    {
        if (len < 0) return null;
        var data = ReadBytes(len);
        ReadByte(); ReadByte();   // CRLF
        return Encoding.UTF8.GetString(data);
    }

    Response ReadReply()
    {
        int t = ReadByte();
        string line = ReadLine();
        switch ((char)t)
        {
            case '+': return Response.Ok(line);
            case '-': return Response.Error(line);
            case ':': return Response.Number(long.Parse(line, CultureInfo.InvariantCulture));
            case '$':
                {
                    var s = ReadBulkBody(int.Parse(line, CultureInfo.InvariantCulture));
                    return s == null ? Response.Nil() : Response.Bulk(s);
                }
            case '*':
                {
                    int n = int.Parse(line, CultureInfo.InvariantCulture);
                    if (n < 0) return Response.Nil();
                    var items = new string?[n];
                    for (int i = 0; i < n; i++)
                    {
                        int et = ReadByte();
                        string el = ReadLine();
                        items[i] = (char)et switch
                        {
                            '$' => ReadBulkBody(int.Parse(el, CultureInfo.InvariantCulture)),
                            ':' or '+' => el,
                            _ => throw new CacheClientException("nested/unsupported array element in reply")
                        };
                    }
                    return Response.List(items);
                }
            default: throw new CacheClientException($"unexpected reply type '{(char)t}'");
        }
    }

    public void Dispose() { lock (_lock) Disconnect(); }
}
