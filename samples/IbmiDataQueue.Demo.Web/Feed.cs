using System.Collections.Concurrent;
using System.Text;

namespace IbmiDataQueue.Demo.Web;

/// <summary>Message métier de la démo : Id(6) + Type(4) + Payload(20) = 30 octets ASCII.</summary>
public sealed record DemoMessage(string Id, string Type, string Payload);

public sealed class DemoCodec : IMessageCodec<DemoMessage>
{
    public static readonly FixedWidthLayout Layout = new(Encoding.ASCII, [6, 4, 20]);

    public DemoMessage Decode(ReadOnlySpan<byte> payload)
    {
        var fields = Layout.Read(payload);
        return new DemoMessage(fields[0], fields[1], fields[2]);
    }

    public static byte[] Encode(string id, string type, string payload) =>
        Encoding.ASCII.GetBytes(id.PadRight(6) + type.PadRight(4) + payload.PadRight(20));
}

public sealed record ReceivedItem(DateTimeOffset At, string Id, string Type, string Payload);

public sealed record DeadLetterItem(DateTimeOffset At, string Reason, string Error, int Length, string Text, string Hex);

/// <summary>Historique en mémoire (derniers 50) de ce que le consommateur a décodé ou rejeté, lu par la page web.</summary>
public sealed class Feed
{
    private const int Max = 50;
    private readonly ConcurrentQueue<ReceivedItem> _received = new();
    private readonly ConcurrentQueue<DeadLetterItem> _dead = new();

    public IReadOnlyList<ReceivedItem> Received => _received.Reverse().ToArray();

    public IReadOnlyList<DeadLetterItem> DeadLetters => _dead.Reverse().ToArray();

    public void Add(ReceivedItem item) => Push(_received, item);

    public void Add(DeadLetterItem item) => Push(_dead, item);

    private static void Push<T>(ConcurrentQueue<T> queue, T item)
    {
        queue.Enqueue(item);
        while (queue.Count > Max && queue.TryDequeue(out _))
        {
        }
    }
}

/// <summary>Traitement « métier » de la démo : on garde le message décodé pour l'afficher.</summary>
public sealed class FeedHandler : IMessageHandler<DemoMessage>
{
    private readonly Feed _feed;

    public FeedHandler(Feed feed) => _feed = feed;

    public ValueTask HandleAsync(DemoMessage message, CancellationToken cancellationToken)
    {
        _feed.Add(new ReceivedItem(DateTimeOffset.Now, message.Id, message.Type, message.Payload));
        return ValueTask.CompletedTask;
    }
}

/// <summary>Un message impossible à décoder : il n'existe plus dans la queue, on garde ses octets.</summary>
public sealed class FeedDeadLetterSink : IDeadLetterSink
{
    private readonly Feed _feed;

    public FeedDeadLetterSink(Feed feed) => _feed = feed;

    public ValueTask WriteAsync(DeadLetter letter, CancellationToken cancellationToken)
    {
        var bytes = letter.Message.Payload.ToArray();
        _feed.Add(new DeadLetterItem(
            DateTimeOffset.Now,
            letter.Reason.ToString(),
            letter.Error.Message,
            bytes.Length,
            Encoding.ASCII.GetString(bytes),
            Convert.ToHexString(bytes)));
        return ValueTask.CompletedTask;
    }
}
