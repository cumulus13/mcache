// File: src/MsmqCache.Server/TcpTransport.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: Redis-compatible TCP server (RESP2): auth, TLS, pipelining, pub/sub, limits, idle timeout
// License: MIT

using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Microsoft.Extensions.Logging;

namespace MsmqCache;

public sealed class TcpTransport
{
    sealed class Session
    {
        public long Id;
        public bool Authed;
        public string? Name;
        public int Failures;
        public int PendingDelayMs;
        public readonly SemaphoreSlim WriteLock = new(1, 1);   // replies and pub/sub pushes share the socket
        public PubSubHub.Subscriber? Sub;
        public Task? Pump;
        public readonly HashSet<string> Channels = new(StringComparer.Ordinal);
    }

    static readonly byte[] MaxClientsReply = Encoding.ASCII.GetBytes("-ERR max number of clients reached\r\n");

    readonly ConfigManager _cfg;
    readonly CommandHandler _handler;
    readonly PubSubHub _hub;
    readonly ServerStats _stats;
    readonly ILogger _log;
    X509Certificate2? _cert;

    ServerConfig C => _cfg.Current;

    public TcpTransport(ConfigManager cfg, CommandHandler handler, PubSubHub hub, ServerStats stats, ILogger log)
    { _cfg = cfg; _handler = handler; _hub = hub; _stats = stats; _log = log; }

    public async Task RunAsync(CancellationToken ct)
    {
        var c = C;
        if (c.TlsEnabled)
        {
            var path = Path.GetFullPath(Environment.ExpandEnvironmentVariables(c.TlsCertificatePath), ConfigManager.BaseDir);
            _cert = new X509Certificate2(path, c.TlsCertificatePassword);
        }

        ConfigManager.TryParseBind(c.BindAddress, out var ip);
        var listener = new TcpListener(ip, c.Port);
        try { listener.Start(511); }
        catch (SocketException ex)
        {
            throw new InvalidOperationException($"Cannot listen on {c.BindAddress}:{c.Port} ({ex.SocketErrorCode}). Is another server (e.g. Redis) using that port?", ex);
        }

        _stats.TcpPort = ((IPEndPoint)listener.LocalEndpoint).Port;
        _log.LogInformation("Listening on {Addr}:{Port} (RESP2{Tls}){Auth}", c.BindAddress, _stats.TcpPort,
            c.TlsEnabled ? ", TLS" : "", string.IsNullOrEmpty(c.Password) ? ", NO PASSWORD" : ", password required");
        if (!IPAddress.IsLoopback(ip) && string.IsNullOrEmpty(c.Password))
            _log.LogWarning("Bound to non-loopback address {Addr} without a Password: anyone who can reach this port has full access.", c.BindAddress);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client;
                try { client = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
                catch (SocketException ex)
                {
                    _log.LogWarning("Accept failed: {Code}", ex.SocketErrorCode);
                    try { await Task.Delay(200, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                    continue;
                }

                if (Interlocked.Read(ref _stats.ConnectedClients) >= C.MaxClients)
                {
                    Interlocked.Increment(ref _stats.RejectedConnections);
                    try { client.Client.Send(MaxClientsReply); } catch { }
                    client.Dispose();
                    continue;
                }

                Interlocked.Increment(ref _stats.ConnectedClients);
                Interlocked.Increment(ref _stats.TotalConnections);
                long id = Interlocked.Increment(ref _stats.NextClientId);
                _ = Task.Run(() => HandleAsync(client, id, ct), CancellationToken.None);
            }
        }
        finally
        {
            listener.Stop();
            for (int i = 0; i < 50 && Interlocked.Read(ref _stats.ConnectedClients) > 0; i++)
                await Task.Delay(100).ConfigureAwait(false);
            _cert?.Dispose();
            _log.LogInformation("TCP listener stopped");
        }
    }

    async Task HandleAsync(TcpClient client, long id, CancellationToken ct)
    {
        var session = new Session { Id = id, Authed = string.IsNullOrEmpty(C.Password) };
        using var kill = CancellationTokenSource.CreateLinkedTokenSource(ct);   // idle timeout / slow-subscriber kill
        try
        {
            client.NoDelay = true;
            if (C.TcpKeepAlive) client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);

            Stream stream = client.GetStream();
            if (C.TlsEnabled)
            {
                var ssl = new SslStream(stream, false);
                using var hs = CancellationTokenSource.CreateLinkedTokenSource(ct);
                hs.CancelAfter(TimeSpan.FromSeconds(10));
                await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                {
                    ServerCertificate = _cert,
                    ClientCertificateRequired = false,
                    EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
                }, hs.Token).ConfigureAwait(false);
                stream = ssl;
            }

            int maxBulk = Math.Max(C.MaxValueLength * 3, 64 * 1024);
            var reader = new RespReader(stream, maxBulk, 100_000);
            var outBuf = new MemoryStream();

            while (!ct.IsCancellationRequested)
            {
                int idleSec = C.IdleTimeoutSeconds;
                // Redis never idle-times-out subscribed clients; neither do we.
                bool useIdle = idleSec > 0 && session.Channels.Count == 0;
                kill.CancelAfter(useIdle ? TimeSpan.FromSeconds(idleSec) : Timeout.InfiniteTimeSpan);

                string[]? cmd;
                try { cmd = await reader.ReadCommandAsync(kill.Token).ConfigureAwait(false); }
                catch (ProtocolException pe)
                {
                    RespWriter.WriteError(outBuf, "Protocol error: " + pe.Message);
                    await FlushAsync(stream, outBuf, session, ct).ConfigureAwait(false);
                    break;
                }
                kill.CancelAfter(Timeout.InfiniteTimeSpan);

                if (cmd == null) break;          // client closed
                if (cmd.Length == 0) continue;   // empty command

                var (resp, close) = await DispatchAsync(session, cmd, stream, kill, ct).ConfigureAwait(false);
                RespWriter.Write(outBuf, resp);

                if (session.PendingDelayMs > 0)
                {
                    await FlushAsync(stream, outBuf, session, ct).ConfigureAwait(false);
                    await Task.Delay(session.PendingDelayMs, ct).ConfigureAwait(false);
                    session.PendingDelayMs = 0;
                }

                // Pipelining: only flush when no more request bytes are already buffered (or output is large).
                if (close || !reader.HasBufferedData || outBuf.Length > 64 * 1024)
                    await FlushAsync(stream, outBuf, session, ct).ConfigureAwait(false);
                if (close) break;
            }
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException
                                      or AuthenticationException or OperationCanceledException)
        {
            _log.LogDebug("Client {Id} ended: {Type}", id, ex.GetType().Name);
        }
        catch (Exception ex)
        {
            _log.LogError(ex, "Client {Id} failed", id);
        }
        finally
        {
            if (session.Sub != null) _hub.Remove(session.Sub, session.Channels);
            if (session.Pump != null) { try { await session.Pump.ConfigureAwait(false); } catch { } }
            Interlocked.Decrement(ref _stats.ConnectedClients);
            client.Dispose();
        }
    }

    static async Task FlushAsync(Stream s, MemoryStream buf, Session session, CancellationToken ct)
    {
        if (buf.Length == 0) return;
        await session.WriteLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await s.WriteAsync(buf.GetBuffer().AsMemory(0, (int)buf.Length), ct).ConfigureAwait(false);
            await s.FlushAsync(ct).ConfigureAwait(false);
        }
        finally { session.WriteLock.Release(); }
        buf.SetLength(0);
    }

    async Task<(Response Resp, bool Close)> DispatchAsync(Session s, string[] cmd, Stream stream,
                                                          CancellationTokenSource kill, CancellationToken ct)
    {
        string name = cmd[0].ToUpperInvariant();
        string[] args = cmd.Length > 1 ? cmd[1..] : Array.Empty<string>();

        if (name == "QUIT") return (Response.Ok(), true);
        if (name == "AUTH") { var r = Auth(s, args, out bool close); return (r, close); }
        if (!s.Authed) return (Response.Error("NOAUTH Authentication required."), false);

        // Subscribed connections may only run the (un)subscribe family, PING and QUIT.
        if (s.Channels.Count > 0 && name is not ("SUBSCRIBE" or "UNSUBSCRIBE" or "PING"))
            return (Response.Error($"Can't execute '{name.ToLowerInvariant()}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context"), false);

        switch (name)
        {
            case "SUBSCRIBE": return (Subscribe(s, args, stream, kill), false);
            case "UNSUBSCRIBE": return (Unsubscribe(s, args), false);
            case "PING" when s.Channels.Count > 0:
                return (Response.Raw(PubSubHub.PongFrame(args.Length > 0 ? args[0] : "")), false);
            case "SELECT":
                if (args.Length != 1) return (Response.Error("wrong number of arguments for 'select' command"), false);
                return (args[0] == "0" ? Response.Ok() : Response.Error("DB index is out of range"), false);
            case "CLIENT":
                if (args.Length == 0) return (Response.Error("wrong number of arguments for 'client' command"), false);
                switch (args[0].ToUpperInvariant())
                {
                    case "SETNAME": if (args.Length != 2) return (Response.Error("wrong number of arguments for 'client|setname' command"), false); s.Name = args[1]; return (Response.Ok(), false);
                    case "GETNAME": return (s.Name == null ? Response.Nil() : Response.Bulk(s.Name), false);
                    case "ID": return (Response.Number(s.Id), false);
                    case "SETINFO": return (Response.Ok(), false);
                    default: return (Response.Error($"unknown subcommand '{args[0]}' for 'client' command"), false);
                }
            case "COMMAND":
                if (args.Length > 0 && args[0].Equals("COUNT", StringComparison.OrdinalIgnoreCase))
                    return (Response.Number(CommandHandler.CommandNames.Length), false);
                return (Response.List(Array.Empty<string?>()), false);   // enough for redis-cli / client handshakes
            default:
                return (await _handler.ExecuteAsync(new Request { Cmd = name, Args = args }, ct).ConfigureAwait(false), false);
        }
    }

    // ---- pub/sub ----
    Response Subscribe(Session s, string[] args, Stream stream, CancellationTokenSource kill)
    {
        if (!C.EnablePubSub) return Response.Error("pub/sub is disabled");
        if (args.Length == 0) return Response.Error("wrong number of arguments for 'subscribe' command");

        if (s.Sub == null)
        {
            s.Sub = _hub.CreateSubscriber();
            var sub = s.Sub;
            sub.OnOverflow = () => { try { kill.Cancel(); } catch (ObjectDisposedException) { } };   // slow subscriber: drop the connection
            s.Pump = Task.Run(() => PumpAsync(s, stream, kill));
        }

        var frames = new MemoryStream();
        foreach (var ch in args)
        {
            if (s.Channels.Add(ch)) _hub.Subscribe(s.Sub, ch);
            frames.Write(PubSubHub.ConfirmFrame("subscribe", ch, s.Channels.Count));
        }
        return Response.Raw(frames.ToArray());
    }

    Response Unsubscribe(Session s, string[] args)
    {
        var frames = new MemoryStream();
        var targets = args.Length > 0 ? args : s.Channels.ToArray();
        if (targets.Length == 0) return Response.Raw(PubSubHub.ConfirmFrame("unsubscribe", null, 0));
        foreach (var ch in targets)
        {
            if (s.Channels.Remove(ch) && s.Sub != null) _hub.Unsubscribe(s.Sub, ch);
            frames.Write(PubSubHub.ConfirmFrame("unsubscribe", ch, s.Channels.Count));
        }
        return Response.Raw(frames.ToArray());
    }

    async Task PumpAsync(Session s, Stream stream, CancellationTokenSource kill)
    {
        try
        {
            await foreach (var frame in s.Sub!.Out.Reader.ReadAllAsync(kill.Token).ConfigureAwait(false))
            {
                await s.WriteLock.WaitAsync(kill.Token).ConfigureAwait(false);
                try
                {
                    await stream.WriteAsync(frame, kill.Token).ConfigureAwait(false);
                    await stream.FlushAsync(kill.Token).ConfigureAwait(false);
                }
                finally { s.WriteLock.Release(); }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
            try { kill.Cancel(); } catch (ObjectDisposedException) { }
        }
    }

    Response Auth(Session s, string[] a, out bool close)
    {
        close = false;
        if (a.Length is < 1 or > 2) return Response.Error("wrong number of arguments for 'auth' command");
        string pw = C.Password ?? "";
        if (pw.Length == 0)
            return Response.Error("AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");

        string user = a.Length == 2 ? a[0] : "default";
        bool ok = user == "default" && FixedEquals(a[^1], pw);
        if (ok) { s.Authed = true; s.Failures = 0; return Response.Ok(); }

        s.PendingDelayMs = 250;                       // slow down brute force
        if (++s.Failures >= 5) close = true;
        return Response.Error("WRONGPASS invalid username-password pair or user is disabled.");
    }

    static bool FixedEquals(string a, string b) =>
        CryptographicOperations.FixedTimeEquals(
            SHA256.HashData(Encoding.UTF8.GetBytes(a)),
            SHA256.HashData(Encoding.UTF8.GetBytes(b)));
}
