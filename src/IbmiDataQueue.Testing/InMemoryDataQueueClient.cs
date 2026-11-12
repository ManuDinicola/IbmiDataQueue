using System.Collections.Concurrent;
using System.Threading.Channels;

namespace IbmiDataQueue.Testing;

/// <summary>Data queue en mémoire, FIFO, avec attente. Se comporte comme QSYS2.RECEIVE_DATA_QUEUE avec REMOVE => 'YES'.</summary>
public sealed class InMemoryDataQueueClient : IDataQueueClient
{
    private readonly ConcurrentDictionary<QueueRef, Channel<DataQueueMessage>> _queues = new();
    private readonly TimeProvider _time;

    public InMemoryDataQueueClient(TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Dépose un message (équivalent de QSYS2.SEND_DATA_QUEUE).</summary>
    public void Send(QueueRef queue, ReadOnlyMemory<byte> payload, string? key = null) =>
        Get(queue).Writer.TryWrite(new DataQueueMessage(payload.ToArray(), key));

    /// <summary>Nombre de messages en attente.</summary>
    public int Count(QueueRef queue) => Get(queue).Reader.Count;

    public async ValueTask<DataQueueMessage?> ReceiveAsync(QueueRef queue, TimeSpan wait, CancellationToken cancellationToken)
    {
        var reader = Get(queue).Reader;
        if (reader.TryRead(out var immediate))
        {
            return immediate;
        }

        if (wait <= TimeSpan.Zero)
        {
            return null;
        }

        if (wait == Timeout.InfiniteTimeSpan)
        {
            return await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }

        using var timeout = new CancellationTokenSource(wait, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            return await reader.ReadAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null; // délai d'attente écoulé : queue restée vide
        }
    }

    private Channel<DataQueueMessage> Get(QueueRef queue) =>
        _queues.GetOrAdd(queue, _ => Channel.CreateUnbounded<DataQueueMessage>());
}
