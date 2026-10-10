// File: src/MsmqCache.Server/PubSubHub.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-09
// Description: In-process pub/sub fan-out with bounded per-subscriber buffers (slow subscribers are disconnected)
// License: MIT

using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading.Channels;

namespace MsmqCache;

public sealed class PubSubHub
{
    public sealed class Subscriber
    {
        public long Id;
        public readonly Channel<byte[]> Out = Channel.CreateBounded<byte[]>(
            new BoundedChannelOptions(1000) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true });
        public Action? OnOverflow;
    }

    readonly ConcurrentDictionary<string, ConcurrentDictionary<long, Subscriber>> _channels = new(StringComparer.Ordinal);
    long _nextId;

    public int ChannelCount => _channels.Count;
    public long SubscriberCount => _channels.Values.Sum(c => (long)c.Count);

    public Subscriber CreateSubscriber() => new() { Id = Interlocked.Increment(ref _nextId) };

    public void Subscribe(Subscriber s, string channel) =>
        _channels.GetOrAdd(channel, _ => new ConcurrentDictionary<long, Subscriber>())[s.Id] = s;

    public void Unsubscribe(Subscriber s, string channel)
    {
        if (_channels.TryGetValue(channel, out var subs))
        {
            subs.TryRemove(s.Id, out _);
            if (subs.IsEmpty) _channels.TryRemove(new KeyValuePair<string, ConcurrentDictionary<long, Subscriber>>(channel, subs));
        }
    }

    public void Remove(Subscriber s, IEnumerable<string> channels)
    {
        foreach (var ch in channels.ToArray()) Unsubscribe(s, ch);
        s.Out.Writer.TryComplete();
    }

    // Returns the number of local subscribers the message was queued for.
    public int Publish(string channel, string message)
    {
        if (!_channels.TryGetValue(channel, out var subs)) return 0;
        var frame = MessageFrame(channel, message);
        int n = 0;
        foreach (var s in subs.Values)
        {
            if (s.Out.Writer.TryWrite(frame)) n++;
            else { try { s.OnOverflow?.Invoke(); } catch { } }   // subscriber too slow: its connection is closed
        }
        return n;
    }

    static void Bulk(MemoryStream ms, string s)
    {
        var b = Encoding.UTF8.GetBytes(s);
        ms.Write(Encoding.ASCII.GetBytes("$" + b.Length.ToString(CultureInfo.InvariantCulture) + "\r\n"));
        ms.Write(b);
        ms.Write(new byte[] { (byte)'\r', (byte)'\n' });
    }

    public static byte[] MessageFrame(string channel, string message)
    {
        var ms = new MemoryStream();
        ms.Write(Encoding.ASCII.GetBytes("*3\r\n"));
        Bulk(ms, "message"); Bulk(ms, channel); Bulk(ms, message);
        return ms.ToArray();
    }

    // subscribe / unsubscribe confirmation: [kind, channel|nil, count]
    public static byte[] ConfirmFrame(string kind, string? channel, int count)
    {
        var ms = new MemoryStream();
        ms.Write(Encoding.ASCII.GetBytes("*3\r\n"));
        Bulk(ms, kind);
        if (channel == null) ms.Write(Encoding.ASCII.GetBytes("$-1\r\n")); else Bulk(ms, channel);
        ms.Write(Encoding.ASCII.GetBytes(":" + count.ToString(CultureInfo.InvariantCulture) + "\r\n"));
        return ms.ToArray();
    }

    public static byte[] PongFrame(string payload)
    {
        var ms = new MemoryStream();
        ms.Write(Encoding.ASCII.GetBytes("*2\r\n"));
        Bulk(ms, "pong"); Bulk(ms, payload);
        return ms.ToArray();
    }
}
