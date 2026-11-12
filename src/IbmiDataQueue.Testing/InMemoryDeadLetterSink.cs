using System.Collections.Concurrent;

namespace IbmiDataQueue.Testing;

public sealed class InMemoryDeadLetterSink : IDeadLetterSink
{
    private readonly ConcurrentQueue<DeadLetter> _items = new();

    public IReadOnlyList<DeadLetter> Items => _items.ToArray();

    public ValueTask WriteAsync(DeadLetter letter, CancellationToken cancellationToken)
    {
        _items.Enqueue(letter);
        return ValueTask.CompletedTask;
    }
}
