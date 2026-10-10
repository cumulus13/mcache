// File: src/MsmqCache.Server/RespWriter.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: RESP2 encoder for Response objects
// License: MIT

using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace MsmqCache;

public static class RespWriter
{
    static readonly byte[] Crlf = { (byte)'\r', (byte)'\n' };
    static readonly byte[] NilBulk = Encoding.ASCII.GetBytes("$-1\r\n");
    static readonly Regex ErrPrefix = new(@"^[A-Z][A-Z0-9_]+ ", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static void Write(Stream o, Response r)
    {
        if (r.RawResp != null) { o.Write(r.RawResp); return; }
        switch (r.Kind)
        {
            case Kinds.Ok: Line(o, '+', Clean(r.Str ?? "OK")); break;
            case Kinds.Num: Line(o, ':', r.Num.ToString(CultureInfo.InvariantCulture)); break;
            case Kinds.Bulk: Bulk(o, r.Str ?? ""); break;
            case Kinds.List:
                var a = r.Arr ?? Array.Empty<string?>();
                Line(o, '*', a.Length.ToString(CultureInfo.InvariantCulture));
                foreach (var x in a) { if (x is null) o.Write(NilBulk); else Bulk(o, x); }
                break;
            case Kinds.Err: Line(o, '-', FormatError(r.Str)); break;
            default: o.Write(NilBulk); break;
        }
    }

    public static void WriteError(Stream o, string message) => Line(o, '-', FormatError(message));

    static string Clean(string s) => s.Replace('\r', ' ').Replace('\n', ' ');

    // Redis errors start with an upper-case code (ERR, NOAUTH, WRONGPASS, OOM...). Keep a code if present, else add ERR.
    static string FormatError(string? m)
    {
        m = Clean(m ?? "error");
        return ErrPrefix.IsMatch(m) ? m : "ERR " + m;
    }

    static void Line(Stream o, char prefix, string s)
    {
        o.WriteByte((byte)prefix);
        o.Write(Encoding.UTF8.GetBytes(s));
        o.Write(Crlf);
    }

    static void Bulk(Stream o, string s)
    {
        var b = Encoding.UTF8.GetBytes(s);
        Line(o, '$', b.Length.ToString(CultureInfo.InvariantCulture));
        o.Write(b);
        o.Write(Crlf);
    }
}
