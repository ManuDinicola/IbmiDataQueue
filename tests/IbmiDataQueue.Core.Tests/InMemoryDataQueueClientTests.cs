using IbmiDataQueue.Testing;
using Xunit;

namespace IbmiDataQueue.Core.Tests;

public class InMemoryDataQueueClientTests
{
    private static readonly QueueRef Queue = new("TESTLIB", "ECHOQ");

    [Fact]
    public async Task Receives_messages_in_fifo_order_and_removes_them()
    {
        var client = new InMemoryDataQueueClient();
        client.Send(Queue, new byte[] { 1 });
        client.Send(Queue, new byte[] { 2 });

        var first = await client.ReceiveAsync(Queue, TimeSpan.Zero, CancellationToken.None);
        var second = await client.ReceiveAsync(Queue, TimeSpan.Zero, CancellationToken.None);

        Assert.Equal(1, first!.Payload.Span[0]);
        Assert.Equal(2, second!.Payload.Span[0]);
        Assert.Equal(0, client.Count(Queue));
    }

    [Fact]
    public async Task Returns_null_when_empty_and_wait_elapses()
    {
        var client = new InMemoryDataQueueClient();

        var message = await client.ReceiveAsync(Queue, TimeSpan.FromMilliseconds(30), CancellationToken.None);

        Assert.Null(message);
    }

    [Fact]
    public async Task Receives_message_that_arrives_during_the_wait()
    {
        var client = new InMemoryDataQueueClient();
        var receive = client.ReceiveAsync(Queue, TimeSpan.FromSeconds(5), CancellationToken.None).AsTask();

        client.Send(Queue, new byte[] { 7 });
        var message = await receive;

        Assert.Equal(7, message!.Payload.Span[0]);
    }

    [Fact]
    public async Task Keeps_queues_separate()
    {
        var client = new InMemoryDataQueueClient();
        client.Send(new QueueRef("TESTLIB", "OTHERQ"), new byte[] { 9 });

        var message = await client.ReceiveAsync(Queue, TimeSpan.Zero, CancellationToken.None);

        Assert.Null(message);
    }
}
