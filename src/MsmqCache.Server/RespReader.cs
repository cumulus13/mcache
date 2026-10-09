// File: src/MsmqCache.Server/RespReader.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: RESP2 request parser (multibulk + inline) with strict limits, UTF-8 strict
// License: MIT

using System.Text;

namespace MsmqCache;

public sealed class ProtocolException : Exception
{
    public ProtocolException(string m) : base(m) { }
}

public sealed class RespReader
{
    static readonly UTF8Encoding StrictUtf8 = new(false, true);

    readonly Stream _s;
    readonly byte[] _buf = new byte[16 * 1024];
    int _pos, _len;
    readonly int _maxBulk, _maxArgs;

    public RespReader(Stream s, int maxBulkBytes, int maxArgs)
    {
        _s = s; _maxBulk = maxBulkBytes; _maxArgs = maxArgs;
    }

    public bool HasBufferedData => _pos < _len;

    async ValueTask<bool> FillAsync(CancellationToken ct)
    {
        if (_pos < _len) return true;
        _pos = 0;
        _len = await _s.ReadAsync(_buf.AsMemory(), ct).ConfigureAwait(false);
        return _len > 0;
    }

    // Reads up to CRLF/LF. Returns null on clean EOF before any byte.
    async ValueTask<byte[]?> ReadLineAsync(int maxLen, CancellationToken ct)
    {
        var line = new List<byte>(64);
        while (true)
        {
            if (!await FillAsync(ct).ConfigureAwait(false))
            {
                if (line.Count == 0) return null;
                throw new EndOfStreamException();
            }
            byte b = _buf[_pos++];
            if (b == (byte)'\n')
            {
                if (line.Count > 0 && line[^1] == (byte)'\r') line.RemoveAt(line.Count - 1);
                return line.ToArray();
            }
            line.Add(b);
            if (line.Count > maxLen) throw new ProtocolException("too big inline request");
        }
    }

    async ValueTask<byte[]> ReadExactAsync(int n, CancellationToken ct)
    {
        var res = new byte[n];
        int got = 0;
        while (got < n)
        {
            if (!await FillAsync(ct).ConfigureAwait(false)) throw new EndOfStreamException();
            int c = Math.Min(n - got, _len - _pos);
            Buffer.BlockCopy(_buf, _pos, res, got, c);
            _pos += c; got += c;
        }
        return res;
    }

    static int ParseInt(byte[]? line, string err)
    {
        if (line == null || !int.TryParse(Encoding.ASCII.GetString(line), out var n)) throw new ProtocolException(err);
        return n;
    }

    // Returns null on EOF, empty array for an empty command (caller ignores it).
    public async ValueTask<string[]?> ReadCommandAsync(CancellationToken ct)
    {
        if (!await FillAsync(ct).ConfigureAwait(false)) return null;

        if (_buf[_pos] == (byte)'*')
        {
            _pos++;
            int n = ParseInt(await ReadLineAsync(32, ct).ConfigureAwait(false), "invalid multibulk length");
            if (n <= 0) return Array.Empty<string>();
            if (n > _maxArgs) throw new ProtocolException("invalid multibulk length");

            var args = new string[n];
            for (int i = 0; i < n; i++)
            {
                if (!await FillAsync(ct).ConfigureAwait(false)) throw new EndOfStreamException();
                if (_buf[_pos] != (byte)'$') throw new ProtocolException($"expected '$', got '{(char)_buf[_pos]}'");
                _pos++;
                int len = ParseInt(await ReadLineAsync(32, ct).ConfigureAwait(false), "invalid bulk length");
                if (len < 0 || len > _maxBulk) throw new ProtocolException("invalid bulk length");
                var data = await ReadExactAsync(len + 2, ct).ConfigureAwait(false);
                if (data[len] != (byte)'\r' || data[len + 1] != (byte)'\n') throw new ProtocolException("invalid bulk terminator");
                try { args[i] = StrictUtf8.GetString(data, 0, len); }
                catch (DecoderFallbackException) { throw new ProtocolException("value is not valid UTF-8 (this server stores UTF-8 text only)"); }
            }
            return args;
        }

        var inline = await ReadLineAsync(64 * 1024, ct).ConfigureAwait(false);
        if (inline == null) return null;
        return SplitInline(Encoding.UTF8.GetString(inline));
    }

    // Inline commands (telnet style): whitespace separated, "double" and 'single' quotes supported.
    static string[] SplitInline(string line)
    {
        var t = new List<string>();
        var sb = new StringBuilder();
        bool any = false; char quote = '\0';
        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];
            if (quote != '\0')
            {
                if (c == '\\' && quote == '"' && i + 1 < line.Length)
                {
                    char n = line[++i];
                    sb.Append(n switch { 'n' => '\n', 'r' => '\r', 't' => '\t', _ => n });
                }
                else if (c == quote) quote = '\0';
                else sb.Append(c);
            }
            else if (c == '"' || c == '\'') { quote = c; any = true; }
            else if (char.IsWhiteSpace(c)) { if (sb.Length > 0 || any) { t.Add(sb.ToString()); sb.Clear(); any = false; } }
            else sb.Append(c);
        }
        if (quote != '\0') throw new ProtocolException("unbalanced quotes in request");
        if (sb.Length > 0 || any) t.Add(sb.ToString());
        return t.ToArray();
    }
}
