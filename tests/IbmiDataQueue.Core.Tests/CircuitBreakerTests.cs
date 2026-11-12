using IbmiDataQueue.Testing;
using Xunit;

namespace IbmiDataQueue.Core.Tests;

public class CircuitBreakerTests
{
    private static readonly CancellationToken None = CancellationToken.None;

    private readonly ManualTimeProvider _time = new();
    private readonly InMemoryCircuitBreakerStore _store = new();

    private CircuitBreaker Create(
        string name = "dep",
        ICircuitBreakerStore? store = null,
        Action<Exception>? onError = null,
        double jitter = 0,
        TimeSpan? healthyCache = null) =>
        new(
            name,
            store ?? _store,
            new CircuitBreakerOptions
            {
                FailureThreshold = 3,
                BreakDuration = TimeSpan.FromSeconds(10),
                Jitter = jitter,
                ProbeTimeout = TimeSpan.FromSeconds(5),
                HealthyCacheDuration = healthyCache ?? TimeSpan.Zero,
                OnError = onError,
            },
            _time);

    private static async Task FailAsync(ICircuitBreaker breaker, int times)
    {
        for (var i = 0; i < times; i++)
        {
            Assert.True(await breaker.TryAcquireAsync(None));
            await breaker.RecordFailureAsync(None);
        }
    }

    private async Task<CircuitBreaker> OpenAsync(string name = "dep")
    {
        var breaker = Create(name);
        await FailAsync(breaker, 3);
        return breaker;
    }

    [Fact]
    public async Task Is_closed_by_default_and_allows_operations()
    {
        var breaker = Create();

        Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));
        Assert.True(await breaker.TryAcquireAsync(None));
    }

    [Fact]
    public async Task Opens_after_the_threshold_of_consecutive_failures()
    {
        var breaker = Create();

        await FailAsync(breaker, 2);
        Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));

        await FailAsync(breaker, 1);
        Assert.Equal(CircuitState.Open, await breaker.GetStateAsync(None));
        Assert.False(await breaker.TryAcquireAsync(None));
    }

    [Fact]
    public async Task A_success_resets_the_consecutive_failures()
    {
        var breaker = Create();

        await FailAsync(breaker, 2);
        await breaker.RecordSuccessAsync(None);
        await FailAsync(breaker, 2);

        Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task Stays_open_until_the_break_duration_has_elapsed()
    {
        var breaker = await OpenAsync();

        _time.Advance(TimeSpan.FromSeconds(9));
        Assert.False(await breaker.TryAcquireAsync(None));
        Assert.Equal(CircuitState.Open, await breaker.GetStateAsync(None));

        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(CircuitState.HalfOpen, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task Half_open_grants_a_single_probe()
    {
        var breaker = await OpenAsync();
        _time.Advance(TimeSpan.FromSeconds(10));

        Assert.True(await breaker.TryAcquireAsync(None));
        Assert.False(await breaker.TryAcquireAsync(None));
        Assert.Equal(CircuitState.Open, await breaker.GetStateAsync(None)); // sonde en cours : plus de sonde disponible
    }

    [Fact]
    public async Task A_successful_probe_closes_the_circuit()
    {
        var breaker = await OpenAsync();
        _time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(await breaker.TryAcquireAsync(None));

        await breaker.RecordSuccessAsync(None);

        Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));
        Assert.True(await breaker.TryAcquireAsync(None));
    }

    [Fact]
    public async Task A_failed_probe_reopens_for_a_full_break()
    {
        var breaker = await OpenAsync();
        _time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(await breaker.TryAcquireAsync(None));

        await breaker.RecordFailureAsync(None);

        _time.Advance(TimeSpan.FromSeconds(9));
        Assert.False(await breaker.TryAcquireAsync(None));
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.True(await breaker.TryAcquireAsync(None));
    }

    [Fact]
    public async Task A_lost_probe_is_replaced_after_the_probe_timeout()
    {
        var breaker = await OpenAsync();
        _time.Advance(TimeSpan.FromSeconds(10));
        Assert.True(await breaker.TryAcquireAsync(None)); // sonde jamais résolue (instance tuée)

        _time.Advance(TimeSpan.FromSeconds(4));
        Assert.False(await breaker.TryAcquireAsync(None));
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.True(await breaker.TryAcquireAsync(None));
    }

    [Fact]
    public async Task A_late_failure_while_open_does_not_extend_the_break()
    {
        var breaker = await OpenAsync();
        _time.Advance(TimeSpan.FromSeconds(5));

        await breaker.RecordFailureAsync(None);

        _time.Advance(TimeSpan.FromSeconds(5));
        Assert.Equal(CircuitState.HalfOpen, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task A_late_success_while_open_does_not_close_the_circuit()
    {
        var breaker = await OpenAsync();

        await breaker.RecordSuccessAsync(None);

        Assert.Equal(CircuitState.Open, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task Breakers_with_the_same_name_and_store_share_their_state()
    {
        var first = Create("shared");
        var second = Create("shared");

        await FailAsync(first, 3);

        Assert.Equal(CircuitState.Open, await second.GetStateAsync(None));
        Assert.False(await second.TryAcquireAsync(None));
    }

    [Fact]
    public async Task Breakers_with_different_names_are_independent()
    {
        var first = Create("a");
        var second = Create("b");

        await FailAsync(first, 3);

        Assert.Equal(CircuitState.Closed, await second.GetStateAsync(None));
    }

    [Fact]
    public async Task Only_one_instance_wins_the_probe()
    {
        for (var round = 0; round < 20; round++)
        {
            var name = "race" + round;
            var instances = Enumerable.Range(0, 6).Select(_ => Create(name)).ToList();
            await FailAsync(instances[0], 3);
            _time.Advance(TimeSpan.FromSeconds(10));

            var results = await Task.WhenAll(instances.Select(i => Task.Run(async () => await i.TryAcquireAsync(None))));

            Assert.Equal(1, results.Count(granted => granted));
        }
    }

    [Fact]
    public async Task Retries_when_the_store_reports_a_write_conflict()
    {
        var breaker = new CircuitBreaker(
            "conflict",
            new ConflictOnceStore(_store),
            new CircuitBreakerOptions { FailureThreshold = 1, BreakDuration = TimeSpan.FromSeconds(10), Jitter = 0, HealthyCacheDuration = TimeSpan.Zero },
            _time);

        await breaker.RecordFailureAsync(None);

        Assert.Equal(CircuitState.Open, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task Fails_open_when_the_store_is_broken()
    {
        var errors = new List<Exception>();
        var breaker = Create(store: new BrokenStore(), onError: errors.Add);

        Assert.True(await breaker.TryAcquireAsync(None));
        Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));
        await breaker.RecordFailureAsync(None);
        await breaker.RecordSuccessAsync(None);

        Assert.Equal(4, errors.Count);
    }

    [Fact]
    public async Task Propagates_cancellation_instead_of_failing_open()
    {
        var breaker = Create(store: new BrokenStore(new OperationCanceledException()));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await breaker.TryAcquireAsync(cts.Token));
    }

    [Fact]
    public async Task Healthy_state_is_cached_to_spare_the_store()
    {
        var counting = new CountingStore(_store);
        var breaker = Create(store: counting, healthyCache: TimeSpan.FromSeconds(1));

        for (var i = 0; i < 100; i++)
        {
            Assert.True(await breaker.TryAcquireAsync(None));
            await breaker.RecordSuccessAsync(None);
            Assert.Equal(CircuitState.Closed, await breaker.GetStateAsync(None));
        }

        Assert.Equal(1, counting.Loads);
        Assert.Equal(0, counting.Saves);
    }

    [Fact]
    public async Task A_failure_invalidates_the_healthy_cache()
    {
        var breaker = Create(healthyCache: TimeSpan.FromSeconds(1));
        Assert.True(await breaker.TryAcquireAsync(None)); // met l'état sain en cache

        await FailAsync(breaker, 3);

        Assert.Equal(CircuitState.Open, await breaker.GetStateAsync(None));
    }

    [Fact]
    public async Task Jitter_lengthens_the_break_within_bounds()
    {
        var breaker = Create(jitter: 0.5);
        await FailAsync(breaker, 3);

        _time.Advance(TimeSpan.FromSeconds(9.9));
        Assert.Equal(CircuitState.Open, await breaker.GetStateAsync(None));

        _time.Advance(TimeSpan.FromSeconds(5.2)); // 15,1 s : au-delà du maximum (10 s x 1,5)
        Assert.Equal(CircuitState.HalfOpen, await breaker.GetStateAsync(None));
    }

    [Fact]
    public void Rejects_an_invalid_threshold()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new CircuitBreaker("x", _store, new CircuitBreakerOptions { FailureThreshold = 0 }));
    }

    private sealed class ConflictOnceStore(ICircuitBreakerStore inner) : ICircuitBreakerStore
    {
        private int _conflicts;

        public ValueTask<CircuitSnapshot> LoadAsync(string name, CancellationToken cancellationToken) =>
            inner.LoadAsync(name, cancellationToken);

        public ValueTask<bool> TrySaveAsync(string name, CircuitSnapshot next, long expectedVersion, CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _conflicts) == 1
                ? ValueTask.FromResult(false)
                : inner.TrySaveAsync(name, next, expectedVersion, cancellationToken);
    }

    private sealed class BrokenStore(Exception? error = null) : ICircuitBreakerStore
    {
        private readonly Exception _error = error ?? new InvalidOperationException("store indisponible");

        public ValueTask<CircuitSnapshot> LoadAsync(string name, CancellationToken cancellationToken) => throw _error;

        public ValueTask<bool> TrySaveAsync(string name, CircuitSnapshot next, long expectedVersion, CancellationToken cancellationToken) =>
            throw _error;
    }

    private sealed class CountingStore(ICircuitBreakerStore inner) : ICircuitBreakerStore
    {
        private int _loads;
        private int _saves;

        public int Loads => Volatile.Read(ref _loads);

        public int Saves => Volatile.Read(ref _saves);

        public ValueTask<CircuitSnapshot> LoadAsync(string name, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _loads);
            return inner.LoadAsync(name, cancellationToken);
        }

        public ValueTask<bool> TrySaveAsync(string name, CircuitSnapshot next, long expectedVersion, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _saves);
            return inner.TrySaveAsync(name, next, expectedVersion, cancellationToken);
        }
    }
}
