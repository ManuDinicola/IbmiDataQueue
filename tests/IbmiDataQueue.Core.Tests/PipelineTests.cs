using IbmiDataQueue.Testing;
using Xunit;

namespace IbmiDataQueue.Core.Tests;

public class PipelineTests
{
    private static readonly QueueRef Queue = new("TESTLIB", "ECHOQ");

    /// <summary>Assemblage d'un pipeline de test : client et dead-letter en mémoire, handler à base de lambda.</summary>
    private sealed class Rig
    {
        public InMemoryDataQueueClient Client { get; } = new();

        public InMemoryDeadLetterSink DeadLetters { get; } = new();

        public List<Echo> Handled { get; } = [];

        public int Calls { get; private set; }

        public DataQueuePipeline<Echo> Pipeline { get; }

        public Rig(
            Func<Echo, int, CancellationToken, ValueTask>? behaviour = null,
            IDeadLetterSink? deadLetters = null,
            IDataQueueClient? client = null,
            Action<PipelineOptions>? configure = null)
        {
            var options = new PipelineOptions
            {
                Wait = TimeSpan.Zero,
                MaxHandlerAttempts = 3,
                RetryBackoff = _ => TimeSpan.Zero,
                ErrorDelay = TimeSpan.FromMilliseconds(5),
            };
            configure?.Invoke(options);

            var handler = new DelegateMessageHandler<Echo>(async (message, ct) =>
            {
                Calls++;
                if (behaviour is not null)
                {
                    await behaviour(message, Calls, ct);
                }

                Handled.Add(message);
            });

            Pipeline = new DataQueuePipeline<Echo>(
                client ?? Client, Queue, new EchoCodec(), handler, deadLetters ?? DeadLetters, options);
        }

        public void Send(byte[] payload) => Client.Send(Queue, payload);
    }

    [Fact]
    public async Task Handles_a_valid_message()
    {
        var rig = new Rig();
        rig.Send(EchoMessages.Make("42", "NEW", "hello"));

        var result = await rig.Pipeline.ProcessNextAsync(CancellationToken.None);

        Assert.Equal(ProcessResult.Handled, result);
        Assert.Equal(new Echo("42", "NEW", "hello"), Assert.Single(rig.Handled));
        Assert.Empty(rig.DeadLetters.Items);
        Assert.Equal(0, rig.Client.Count(Queue));
    }

    [Fact]
    public async Task Reports_no_message_when_queue_is_empty()
    {
        var rig = new Rig();

        var result = await rig.Pipeline.ProcessNextAsync(CancellationToken.None);

        Assert.Equal(ProcessResult.NoMessage, result);
        Assert.Equal(0, rig.Calls);
    }

    [Fact]
    public async Task Dead_letters_a_message_that_is_too_short_and_keeps_its_bytes()
    {
        var rig = new Rig();
        var raw = new byte[] { 1, 2, 3 };
        rig.Send(raw);

        var result = await rig.Pipeline.ProcessNextAsync(CancellationToken.None);

        var letter = Assert.Single(rig.DeadLetters.Items);
        Assert.Equal(ProcessResult.DeadLettered, result);
        Assert.Equal(DeadLetterReason.DecodeFailed, letter.Reason);
        Assert.Equal(0, letter.Attempts);
        Assert.Equal(raw, letter.Message.Payload.ToArray());
        Assert.Equal(0, rig.Calls);
    }

    [Fact]
    public async Task Dead_letters_a_message_that_is_too_long()
    {
        var rig = new Rig();
        rig.Send(new byte[31]);

        var result = await rig.Pipeline.ProcessNextAsync(CancellationToken.None);

        Assert.Equal(ProcessResult.DeadLettered, result);
        Assert.Equal(DeadLetterReason.DecodeFailed, Assert.Single(rig.DeadLetters.Items).Reason);
    }

    [Fact]
    public async Task Retries_then_dead_letters_when_the_handler_always_fails()
    {
        var rig = new Rig((_, _, _) => throw new InvalidOperationException("boom"));
        rig.Send(EchoMessages.Make("1", "NEW", "x"));

        var result = await rig.Pipeline.ProcessNextAsync(CancellationToken.None);

        var letter = Assert.Single(rig.DeadLetters.Items);
        Assert.Equal(ProcessResult.DeadLettered, result);
        Assert.Equal(3, rig.Calls);
        Assert.Equal(DeadLetterReason.HandlerFailed, letter.Reason);
        Assert.Equal(3, letter.Attempts);
        Assert.IsType<InvalidOperationException>(letter.Error);
    }

    [Fact]
    public async Task Succeeds_on_a_later_attempt_without_dead_letter()
    {
        var rig = new Rig((_, call, _) => call == 1 ? throw new InvalidOperationException("transient") : ValueTask.CompletedTask);
        rig.Send(EchoMessages.Make("1", "NEW", "x"));

        var result = await rig.Pipeline.ProcessNextAsync(CancellationToken.None);

        Assert.Equal(ProcessResult.Handled, result);
        Assert.Equal(2, rig.Calls);
        Assert.Empty(rig.DeadLetters.Items);
    }

    [Fact]
    public async Task Keeps_the_message_when_shutdown_interrupts_the_handler()
    {
        using var cts = new CancellationTokenSource();
        var started = new TaskCompletionSource();
        var rig = new Rig(async (_, _, ct) =>
        {
            started.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        });
        rig.Send(EchoMessages.Make("1", "NEW", "x"));

        var processing = rig.Pipeline.ProcessNextAsync(cts.Token).AsTask();
        await started.Task;
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => processing);
        var letter = Assert.Single(rig.DeadLetters.Items);
        Assert.Equal(DeadLetterReason.Interrupted, letter.Reason);
    }

    [Fact]
    public async Task Surfaces_a_failure_of_the_dead_letter_sink()
    {
        var rig = new Rig(deadLetters: new ThrowingSink());
        rig.Send(new byte[3]);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => rig.Pipeline.ProcessNextAsync(CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task Run_processes_messages_until_cancelled()
    {
        using var cts = new CancellationTokenSource();
        var done = new TaskCompletionSource();
        var rig = new Rig(
            (_, call, _) =>
            {
                if (call == 3)
                {
                    done.SetResult();
                }

                return ValueTask.CompletedTask;
            },
            configure: o => o.Wait = TimeSpan.FromMilliseconds(50));
        for (var i = 0; i < 3; i++)
        {
            rig.Send(EchoMessages.Make(i.ToString(), "NEW", "x"));
        }

        var run = rig.Pipeline.RunAsync(cts.Token);
        var finished = await Task.WhenAny(done.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        cts.Cancel();
        await run;

        Assert.Same(done.Task, finished);
        Assert.Equal(3, rig.Handled.Count);
    }

    [Fact]
    public async Task Run_reports_a_receive_error_and_keeps_going()
    {
        using var cts = new CancellationTokenSource();
        var done = new TaskCompletionSource();
        var errors = new List<Exception>();
        var inner = new InMemoryDataQueueClient();
        var rig = new Rig(
            (_, _, _) =>
            {
                done.SetResult();
                return ValueTask.CompletedTask;
            },
            client: new FlakyClient(inner),
            configure: o =>
            {
                o.Wait = TimeSpan.FromMilliseconds(20);
                o.OnError = errors.Add;
            });
        inner.Send(Queue, EchoMessages.Make("1", "NEW", "x"));

        var run = rig.Pipeline.RunAsync(cts.Token);
        var finished = await Task.WhenAny(done.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        cts.Cancel();
        await run;

        Assert.Same(done.Task, finished);
        Assert.Single(errors);
    }

    private sealed class ThrowingSink : IDeadLetterSink
    {
        public ValueTask WriteAsync(DeadLetter letter, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("dead-letter indisponible");
    }

    /// <summary>Échoue au premier appel (connexion perdue), puis se comporte normalement.</summary>
    private sealed class FlakyClient : IDataQueueClient
    {
        private readonly IDataQueueClient _inner;
        private int _calls;

        public FlakyClient(IDataQueueClient inner)
        {
            _inner = inner;
        }

        public ValueTask<DataQueueMessage?> ReceiveAsync(QueueRef queue, TimeSpan wait, CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _calls) == 1
                ? throw new InvalidOperationException("connexion perdue")
                : _inner.ReceiveAsync(queue, wait, cancellationToken);
    }
}
