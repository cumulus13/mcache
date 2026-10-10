// File: tests/MsmqCache.Tests/TcpServerTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: End-to-end tests over real TCP: RESP framing, inline commands, pipelining, limits, UTF-8, clients
// License: MIT

using System.Net.Sockets;
using System.Text;
using MsmqCache;
using Xunit;

public class TcpServerTests : IClassFixture<OpenServerFixture>
{
    readonly OpenServerFixture _fx;
    public TcpServerTests(OpenServerFixture fx) => _fx = fx;

    // ---- raw socket helpers ----
    TcpClient Connect()
    {
        var c = new TcpClient("127.0.0.1", _fx.Port) { NoDelay = true, ReceiveTimeout = 5000, SendTimeout = 5000 };
        return c;
    }

    static void Send(TcpClient c, string s) => c.GetStream().Write(Encoding.UTF8.GetBytes(s));

    static string ReadExact(TcpClient c, int n)
    {
        var buf = new byte[n]; int got = 0; var s = c.GetStream();
        while (got < n)
        {
            int r = s.Read(buf, got, n - got);
            if (r <= 0) throw new IOException("closed");
            got += r;
        }
        return Encoding.UTF8.GetString(buf);
    }

    static string ReadLine(TcpClient c)
    {
        var sb = new StringBuilder(); var s = c.GetStream();
        while (true)
        {
            int b = s.ReadByte();
            if (b < 0) return sb.ToString();
            sb.Append((char)b);
            if (sb.Length >= 2 && sb[^2] == '\r' && sb[^1] == '\n') return sb.ToString();
        }
    }

    static string Cmd(params string[] a) =>
        $"*{a.Length}\r\n" + string.Concat(a.Select(x => $"${Encoding.UTF8.GetByteCount(x)}\r\n{x}\r\n"));

    TcpCacheClient NewClient(string pw = "") => new(new TcpClientOptions { Port = _fx.Port, Password = pw });

    [Fact]
    public void Ping_Resp_And_Inline()
    {
        using var c = Connect();
        Send(c, Cmd("PING"));
        Assert.Equal("+PONG\r\n", ReadLine(c));
        Send(c, "PING\r\n");                      // telnet-style inline
        Assert.Equal("+PONG\r\n", ReadLine(c));
    }

    [Fact]
    public void Set_Get_Missing_Resp()
    {
        using var c = Connect();
        Send(c, Cmd("SET", "k1", "v1"));
        Assert.Equal("+OK\r\n", ReadLine(c));
        Send(c, Cmd("GET", "k1"));
        Assert.Equal("$2\r\nv1\r\n", ReadExact(c, 8));
        Send(c, Cmd("GET", "nope"));
        Assert.Equal("$-1\r\n", ReadLine(c));
    }

    [Fact]
    public void Pipelining_ThreeCommandsOneWrite()
    {
        using var c = Connect();
        Send(c, Cmd("PING") + Cmd("SET", "p", "1") + Cmd("INCR", "p"));
        Assert.Equal("+PONG\r\n+OK\r\n:2\r\n", ReadExact(c, 7 + 5 + 4));
    }

    [Fact]
    public void Unknown_Command_Is_Error_And_Connection_Survives()
    {
        using var c = Connect();
        Send(c, Cmd("NOSUCH"));
        Assert.StartsWith("-ERR unknown command", ReadLine(c));
        Send(c, Cmd("PING"));
        Assert.Equal("+PONG\r\n", ReadLine(c));
    }

    [Fact]
    public void ValueTooLarge_IsSoftError()
    {
        using var c = Connect();
        Send(c, Cmd("SET", "big", new string('x', 2000)));   // MaxValueLength = 1000 in fixture
        Assert.StartsWith("-ERR value too large", ReadLine(c));
        Send(c, Cmd("PING"));
        Assert.Equal("+PONG\r\n", ReadLine(c));
    }

    [Fact]
    public void InvalidUtf8_IsProtocolError_AndClosesConnection()
    {
        using var c = Connect();
        var s = c.GetStream();
        var bytes = new List<byte>();
        bytes.AddRange(Encoding.ASCII.GetBytes("*2\r\n$3\r\nGET\r\n$2\r\n"));
        bytes.AddRange(new byte[] { 0xFF, 0xFE });
        bytes.AddRange(Encoding.ASCII.GetBytes("\r\n"));
        s.Write(bytes.ToArray());
        Assert.StartsWith("-ERR Protocol error", ReadLine(c));
        Assert.Equal(-1, s.ReadByte());                      // server closed
    }

    [Fact]
    public void BulkLengthBeyondLimit_IsProtocolError()
    {
        using var c = Connect();
        Send(c, "*2\r\n$3\r\nGET\r\n$99999999\r\n");
        Assert.StartsWith("-ERR Protocol error", ReadLine(c));
    }

    [Fact]
    public void Quit_ClosesConnection()
    {
        using var c = Connect();
        Send(c, Cmd("QUIT"));
        Assert.Equal("+OK\r\n", ReadLine(c));
        Assert.Equal(-1, c.GetStream().ReadByte());
    }

    [Fact]
    public void Client_Utf8_RoundTrip_And_Ttl()
    {
        using var cl = NewClient();
        Assert.True(cl.Set("u", "héllo ✓ 日本"));
        Assert.Equal("héllo ✓ 日本", cl.Get("u"));
        Assert.True(cl.Set("t", "x", TimeSpan.FromMilliseconds(200)));
        Assert.InRange(cl.Ttl("t"), 0, 1);
        Thread.Sleep(600);
        Assert.Null(cl.Get("t"));
    }

    [Fact]
    public void Client_Concurrent_Incr_IsAtomic()
    {
        Parallel.For(0, 8, _ =>
        {
            using var cl = NewClient();
            for (int i = 0; i < 50; i++) cl.Incr("counter");
        });
        using var check = NewClient();
        Assert.Equal("400", check.Get("counter"));
    }

    [Fact]
    public void Arrays_Mget_Keys_And_Config()
    {
        using var cl = NewClient();
        cl.Execute("MSET", "ak:1", "a", "ak:2", "b");
        var m = cl.Execute("MGET", "ak:1", "zz", "ak:2");
        Assert.Equal(Kinds.List, m.Kind);
        Assert.Equal(new string?[] { "a", null, "b" }, m.Arr);
        Assert.Equal(2, cl.Execute("KEYS", "ak:*").Arr!.Length);

        var cg = cl.Execute("CONFIG", "GET", "maxvaluelength");
        Assert.Equal(new string?[] { "maxvaluelength", "1000" }, cg.Arr);
        Assert.Equal(new string?[] { "password", "" }, cl.Execute("CONFIG", "GET", "password").Arr);
    }

    [Fact]
    public void Redis_Handshake_Commands_Are_Accepted()
    {
        using var cl = NewClient();
        Assert.Equal(Kinds.Ok, cl.Execute("CLIENT", "SETNAME", "tests").Kind);
        Assert.Equal("tests", cl.Execute("CLIENT", "GETNAME").Str);
        Assert.Equal(Kinds.Ok, cl.Execute("SELECT", "0").Kind);
        Assert.Equal(Kinds.Err, cl.Execute("SELECT", "1").Kind);
        Assert.Equal(Kinds.List, cl.Execute("COMMAND").Kind);
        Assert.Equal("hi", cl.Execute("ECHO", "hi").Str);
        Assert.Contains("connected_clients:", cl.Execute("INFO").Str);
    }

    [Fact]
    public void Save_WritesSnapshotFile()
    {
        using var cl = NewClient();
        cl.Set("persist-me", "1");
        Assert.Equal(Kinds.Ok, cl.Execute("SAVE").Kind);
        Assert.True(File.Exists(_fx.Config.SnapshotPath));
        Assert.Contains("persist-me", File.ReadAllText(_fx.Config.SnapshotPath));
    }
}

public class TcpAuthTests : IClassFixture<SecuredServerFixture>
{
    readonly SecuredServerFixture _fx;
    public TcpAuthTests(SecuredServerFixture fx) => _fx = fx;

    TcpCacheClient NewClient(string pw) => new(new TcpClientOptions { Port = _fx.Port, Password = pw });

    [Fact]
    public void NoAuth_Is_Rejected_With_NOAUTH()
    {
        using var c = new TcpClient("127.0.0.1", _fx.Port) { ReceiveTimeout = 5000 };
        var s = c.GetStream();
        s.Write(Encoding.ASCII.GetBytes("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"));
        var buf = new byte[64]; int n = s.Read(buf, 0, buf.Length);
        Assert.StartsWith("-NOAUTH", Encoding.ASCII.GetString(buf, 0, n));
    }

    [Fact]
    public void WrongPassword_Fails_And_RightPassword_Works()
    {
        using var bad = NewClient("nope");
        var ex = Assert.Throws<CacheClientException>(() => bad.Execute("PING"));
        Assert.Contains("WRONGPASS", ex.Message);

        using var good = NewClient("s3cret-pw");
        Assert.True(good.Ping());
        Assert.True(good.Set("a", "b"));
        Assert.Equal("b", good.Get("a"));
    }

    [Fact]
    public void ConfigGet_Never_Reveals_Password()
    {
        using var cl = NewClient("s3cret-pw");
        Assert.Equal(new string?[] { "password", "(hidden)" }, cl.Execute("CONFIG", "GET", "password").Arr);
        Assert.DoesNotContain("s3cret-pw", string.Join(",", cl.Execute("CONFIG", "GET", "*").Arr!));
    }
}

public class TcpPubSubTests : IClassFixture<OpenServerFixture>
{
    readonly OpenServerFixture _fx;
    public TcpPubSubTests(OpenServerFixture fx) => _fx = fx;

    TcpClient Connect() => new("127.0.0.1", _fx.Port) { NoDelay = true, ReceiveTimeout = 5000, SendTimeout = 5000 };
    static void Send(TcpClient c, string s) => c.GetStream().Write(Encoding.UTF8.GetBytes(s));
    static string Cmd(params string[] a) =>
        $"*{a.Length}\r\n" + string.Concat(a.Select(x => $"${Encoding.UTF8.GetByteCount(x)}\r\n{x}\r\n"));

    static string ReadExact(TcpClient c, int n)
    {
        var buf = new byte[n]; int got = 0; var s = c.GetStream();
        while (got < n) { int r = s.Read(buf, got, n - got); if (r <= 0) throw new IOException("closed"); got += r; }
        return Encoding.UTF8.GetString(buf);
    }

    [Fact]
    public void Subscribe_Receives_Published_Messages_And_Restricts_Commands()
    {
        using var sub = Connect();
        Send(sub, Cmd("SUBSCRIBE", "news"));
        string confirm = "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n";
        Assert.Equal(confirm, ReadExact(sub, confirm.Length));

        using var pub = new TcpCacheClient(new TcpClientOptions { Port = _fx.Port });
        Assert.Equal(1, pub.Execute("PUBLISH", "news", "hello").Num);

        string msg = "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
        Assert.Equal(msg, ReadExact(sub, msg.Length));

        // a subscribed connection may only (un)subscribe / PING / QUIT
        Send(sub, Cmd("GET", "x"));
        var errBuf = new byte[64]; int n = sub.GetStream().Read(errBuf, 0, errBuf.Length);
        Assert.StartsWith("-ERR Can't execute 'get'", Encoding.UTF8.GetString(errBuf, 0, n));

        Send(sub, Cmd("PING"));
        string pong = "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
        Assert.Equal(pong, ReadExact(sub, pong.Length));

        Send(sub, Cmd("UNSUBSCRIBE"));
        string unsub = "*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n";
        Assert.Equal(unsub, ReadExact(sub, unsub.Length));

        Assert.Equal(0, pub.Execute("PUBLISH", "news", "nobody").Num);
        Send(sub, Cmd("PING"));                       // normal mode again
        Assert.Equal("+PONG\r\n", ReadExact(sub, 7));
    }

    [Fact]
    public void Two_Subscribers_Both_Receive_And_Count_Is_Reported()
    {
        using var a = Connect(); using var b = Connect();
        Send(a, Cmd("SUBSCRIBE", "multi")); Send(b, Cmd("SUBSCRIBE", "multi"));
        string confirm = "*3\r\n$9\r\nsubscribe\r\n$5\r\nmulti\r\n:1\r\n";
        ReadExact(a, confirm.Length); ReadExact(b, confirm.Length);

        using var pub = new TcpCacheClient(new TcpClientOptions { Port = _fx.Port });
        Assert.Equal(2, pub.Execute("PUBLISH", "multi", "x").Num);
        string msg = "*3\r\n$7\r\nmessage\r\n$5\r\nmulti\r\n$1\r\nx\r\n";
        Assert.Equal(msg, ReadExact(a, msg.Length));
        Assert.Equal(msg, ReadExact(b, msg.Length));
    }

    [Fact]
    public void Disconnected_Subscriber_Is_Removed()
    {
        using (var s = Connect())
        {
            Send(s, Cmd("SUBSCRIBE", "gone"));
            string confirm = "*3\r\n$9\r\nsubscribe\r\n$4\r\ngone\r\n:1\r\n";
            ReadExact(s, confirm.Length);
        }
        using var pub = new TcpCacheClient(new TcpClientOptions { Port = _fx.Port });
        long n = 1;
        for (int i = 0; i < 40 && n != 0; i++) { n = pub.Execute("PUBLISH", "gone", "x").Num; if (n != 0) Thread.Sleep(50); }
        Assert.Equal(0, n);
    }
}
