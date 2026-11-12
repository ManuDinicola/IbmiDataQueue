using IbmiDataQueue.Testing;
using Xunit;

namespace IbmiDataQueue.Core.Tests;

public class ResiliencePipelineTests
{
    private static readonly QueueRef Queue = new("TESTLIB", "ECHOQ");
    private static readonly CancellationToken None = CancellationToken.None;

    private readonly InMemoryDataQueueClient _client = new();
    private readonly InMemoryDeadLetterSink _deadLetters = new();
    private int _calls;

    private static PipelineOptions FastOptions() => new()
    {
        Wait = TimeSpan.Zero,
        MaxHandlerAttempts = 3,
        RetryBackoff = _ => TimeSpan.Zero,
        CircuitOpenDelay = TimeSpan.FromMilliseconds(1),
        ErrorDelay = TimeSpan.FromMilliseconds(5),
    };

    private static CircuitBreaker Breaker(string name, TimeProvider time, int threshold, TimeSpan breakDuration) =>
        new(
            name,
            new InMemoryCircuitBreakerStore(),
            new CircuitBreakerOptions
            {
                FailureThreshold = threshold,
                BreakDuration = breakDuration,
                Jitter = 0,
                ProbeTimeout = TimeSpan.FromSeconds(30),
                HealthyCacheDuration = TimeSpan.Zero,
            },
            time);

    private DataQueuePipeline<Echo> Pipeline(Func<int, ValueTask> behaviour, PipelineOptions options, TimeProvider? time = null) =>
        new(
            _client,
            Queue,
            new EchoCodec(),
            new DelegateMessageHandler<Echo>(async (_, _) => await behaviour(Interlocked.Increment(ref _calls))),
            _deadLetters,
            options,
            time);

    private void SendOne() => _client.Send(Queue, EchoMessages.Make("1", "NEW", "x"));

    [Fact]
    public async Task Transient_failures_are_replayed_until_the_dependency_returns_and_never_dead_lettered()
    {
        var breaker = Breaker("db", TimeProvider.System, threshold: 3, breakDuration: TimeSpan.FromMilliseconds(30));
        var options = FastOptions();
        options.HandlerBreaker = breaker;
        var pipeline = Pipeline(call => call <= 5 ? throw new TimeoutException("base indisponible") : ValueTask.CompletedTask, options);
        SendOne();

        var result = await pipeline.ProcessNextAsync(None);

        Assert.Equal(ProcessResult.Handled, result);
        Assert.Equal(6, _calls);
        Assert.Empty(_deadLetters.Items);
        Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task Transient_failures_do_not_consume_the_permanent_attempts()
    {
        var options = FastOptions();
        options.MaxHandlerAttempts = 1;
        var pipeline = Pipeline(call => call <= 2 ? throw new TimeoutException() : ValueTask.CompletedTask, options);
        SendOne();

        var result = await pipeline.ProcessNextAsync(None);

        Assert.Equal(ProcessResult.Handled, result);
        Assert.Equal(3, _calls);
        Assert.Empty(_deadLetters.Items);
    }

    [Fact]
    public async Task An_open_handler_breaker_stops_reading_and_leaves_the_message_in_the_queue()
    {
        var time = new ManualTimeProvider();
        var breaker = Breaker("db", time, threshold: 2, breakDuration: TimeSpan.FromSeconds(10));
        for (var i = 0; i < 2; i++)
        {
            Assert.True(await breaker.TryAcquireAsync(None));
            await breaker.RecordFailureAsync(None);
        }

        var options = FastOptions();
        options.HandlerBreaker = breaker;
        var pipeline = Pipeline(_ => ValueTask.CompletedTask, options, time);
        SendOne();

        var blocked = await pipeline.ProcessNextAsync(None);

        Assert.Equal(ProcessResult.CircuitOpen, blocked);
        Assert.Equal(1, _client.Count(Queue)); // le message est resté sur IBM i
        Assert.Equal(0, _calls);

        time.Advance(TimeSpan.FromSeconds(10));
        var resumed = await pipeline.ProcessNextAsync(None);

        Assert.Equal(ProcessResult.Handled, resumed);
        Assert.Equal(0, _client.Count(Queue));
        Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task Permanent_failures_go_to_the_dead_letter_without_opening_the_breaker()
    {
        var breaker = Breaker("db", TimeProvider.System, threshold: 2, breakDuration: TimeSpan.FromSeconds(10));
        var options = FastOptions();
        options.HandlerBreaker = breaker;
        var pipeline = Pipeline(_ => throw new InvalidOperationException("message mauvais"), options);
        SendOne();

        var result = await pipeline.ProcessNextAsync(None);

        var letter = Assert.Single(_deadLetters.Items);
        Assert.Equal(ProcessResult.DeadLettered, result);
        Assert.Equal(DeadLetterReason.HandlerFailed, letter.Reason);
        Assert.Equal(3, letter.Attempts);
        Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task A_transient_failure_goes_to_the_dead_letter_only_after_the_hold_is_exceeded()
    {
        var time = new ManualTimeProvider();
        var options = FastOptions();
        options.MaxTransientHold = TimeSpan.FromHours(1);
        var pipeline = Pipeline(
            _ =>
            {
                time.Advance(TimeSpan.FromHours(2));
                throw new TimeoutException("toujours en panne");
            },
            options,
            time);
        SendOne();

        var result = await pipeline.ProcessNextAsync(None);

        var letter = Assert.Single(_deadLetters.Items);
        Assert.Equal(ProcessResult.DeadLettered, result);
        Assert.Equal(DeadLetterReason.HandlerFailed, letter.Reason);
        Assert.Equal(1, letter.Attempts);
        Assert.IsType<TimeoutException>(letter.Error);
    }

    [Fact]
    public async Task Read_errors_open_the_receive_breaker_and_stop_calling_the_client()
    {
        var time = new ManualTimeProvider();
        var failing = new FailingClient(failures: 2);
        var options = FastOptions();
        options.ReceiveBreaker = Breaker("ibmi", time, threshold: 2, breakDuration: TimeSpan.FromSeconds(10));
        var pipeline = new DataQueuePipeline<Echo>(
            failing, Queue, new EchoCodec(), new DelegateMessageHandler<Echo>((_, _) => ValueTask.CompletedTask), _deadLetters, options, time);

        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.ProcessNextAsync(None).AsTask());
        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.ProcessNextAsync(None).AsTask());
        var blocked = await pipeline.ProcessNextAsync(None);

        Assert.Equal(ProcessResult.CircuitOpen, blocked);
        Assert.Equal(2, failing.Calls); // plus aucun appel pendant que le disjoncteur est ouvert

        time.Advance(TimeSpan.FromSeconds(10));
        var probe = await pipeline.ProcessNextAsync(None); // la sonde réussit (queue vide)

        Assert.Equal(ProcessResult.NoMessage, probe);
        Assert.Equal(3, failing.Calls);
        Assert.Equal(CircuitState.Closed, await options.ReceiveBreaker.GetStateAsync(None));
    }

    [Fact]
    public async Task Run_waits_while_the_circuit_is_open_then_resumes()
    {
        var breaker = Breaker("db", TimeProvider.System, threshold: 1, breakDuration: TimeSpan.FromMilliseconds(40));
        Assert.True(await breaker.TryAcquireAsync(None));
        await breaker.RecordFailureAsync(None); // ouvert pour 40 ms
        var handled = new TaskCompletionSource();
        var options = FastOptions();
        options.Wait = TimeSpan.FromMilliseconds(20);
        options.HandlerBreaker = breaker;
        var pipeline = Pipeline(
            _ =>
            {
                handled.TrySetResult();
                return ValueTask.CompletedTask;
            },
            options);
        SendOne();
        using var cts = new CancellationTokenSource();

        var run = pipeline.RunAsync(cts.Token);
        var finished = await Task.WhenAny(handled.Task, Task.Delay(TimeSpan.FromSeconds(5)));
        cts.Cancel();
        await run;

        Assert.Same(handled.Task, finished);
        Assert.Empty(_deadLetters.Items);
    }

    [Fact]
    public void Rejects_an_invalid_concurrency()
    {
        var options = FastOptions();
        options.Concurrency = 0;

        Assert.Throws<ArgumentOutOfRangeException>(() => Pipeline(_ => ValueTask.CompletedTask, options));
    }

    [Fact]
    public void Default_classification_treats_timeouts_and_io_as_transient_and_bugs_as_permanent()
    {
        Assert.True(PipelineOptions.DefaultIsTransient(new TimeoutException()));
        Assert.True(PipelineOptions.DefaultIsTransient(new IOException()));
        Assert.True(PipelineOptions.DefaultIsTransient(new HttpRequestException()));
        Assert.True(!PipelineOptions.DefaultIsTransient(new InvalidOperationException()));
        Assert.True(!PipelineOptions.DefaultIsTransient(new FormatException()));
    }

    private sealed class FailingClient(int failures) : IDataQueueClient
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public ValueTask<DataQueueMessage?> ReceiveAsync(QueueRef queue, TimeSpan wait, CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _calls) <= failures
                ? throw new InvalidOperationException("connexion perdue")
                : ValueTask.FromResult<DataQueueMessage?>(null);
    }
}
