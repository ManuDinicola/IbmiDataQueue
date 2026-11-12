using Xunit;

namespace IbmiDataQueue.Core.Tests;

/// <summary>
/// Contrat commun à tous les <see cref="ICircuitBreakerStore"/> (mémoire, fichiers, SQL Server...).
/// Une implémentation l'hérite et fournit <see cref="CreateStore"/>.
/// </summary>
public abstract class CircuitBreakerStoreContract
{
    private static readonly CancellationToken None = CancellationToken.None;

    protected abstract ICircuitBreakerStore CreateStore();

    protected static string UniqueName() => "cb" + Guid.NewGuid().ToString("N")[..12];

    [Fact]
    public async Task Load_returns_the_initial_state_for_an_unknown_name()
    {
        var snapshot = await CreateStore().LoadAsync(UniqueName(), None);

        Assert.Equal(CircuitSnapshot.Initial, snapshot);
    }

    [Fact]
    public async Task First_save_with_version_zero_creates_the_state_at_version_one()
    {
        var store = CreateStore();
        var name = UniqueName();

        var saved = await store.TrySaveAsync(name, new CircuitSnapshot(CircuitState.Open, 5, 100, 0, 999), 0, None);
        var loaded = await store.LoadAsync(name, None);

        Assert.True(saved);
        Assert.Equal(new CircuitSnapshot(CircuitState.Open, 5, 100, 0, 1), loaded);
    }

    [Fact]
    public async Task Save_with_a_stale_version_is_rejected_and_changes_nothing()
    {
        var store = CreateStore();
        var name = UniqueName();
        Assert.True(await store.TrySaveAsync(name, new CircuitSnapshot(CircuitState.Closed, 1, 0, 0, 0), 0, None));

        var stale = await store.TrySaveAsync(name, new CircuitSnapshot(CircuitState.Open, 9, 0, 0, 0), 0, None);

        Assert.True(!stale);
        Assert.Equal(1, (await store.LoadAsync(name, None)).ConsecutiveFailures);
    }

    [Fact]
    public async Task Save_with_the_current_version_succeeds_and_increments_it()
    {
        var store = CreateStore();
        var name = UniqueName();
        Assert.True(await store.TrySaveAsync(name, new CircuitSnapshot(CircuitState.Closed, 1, 0, 0, 0), 0, None));

        Assert.True(await store.TrySaveAsync(name, new CircuitSnapshot(CircuitState.Closed, 2, 0, 0, 0), 1, None));

        var loaded = await store.LoadAsync(name, None);
        Assert.Equal(2, loaded.Version);
        Assert.Equal(2, loaded.ConsecutiveFailures);
    }

    [Fact]
    public async Task Round_trips_every_field_including_large_tick_values()
    {
        var store = CreateStore();
        var name = UniqueName();
        var next = new CircuitSnapshot(CircuitState.HalfOpen, 7, 639_000_000_000_000_000L, 639_000_005_000_000_000L, 0);

        Assert.True(await store.TrySaveAsync(name, next, 0, None));

        Assert.Equal(next with { Version = 1 }, await store.LoadAsync(name, None));
    }

    [Fact]
    public async Task Names_are_independent()
    {
        var store = CreateStore();
        var first = UniqueName();
        var second = UniqueName();

        Assert.True(await store.TrySaveAsync(first, new CircuitSnapshot(CircuitState.Open, 3, 1, 0, 0), 0, None));

        Assert.Equal(CircuitSnapshot.Initial, await store.LoadAsync(second, None));
    }

    [Fact]
    public async Task Names_with_special_characters_are_supported()
    {
        var store = CreateStore();
        var name = "a/b\\c:d*e?\"<>|é 日本 " + UniqueName();

        Assert.True(await store.TrySaveAsync(name, new CircuitSnapshot(CircuitState.Open, 1, 1, 0, 0), 0, None));

        Assert.Equal(CircuitState.Open, (await store.LoadAsync(name, None)).State);
    }

    [Fact]
    public async Task Concurrent_saves_with_the_same_version_have_exactly_one_winner()
    {
        var store = CreateStore();
        var name = UniqueName();

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(i =>
            Task.Run(async () => await store.TrySaveAsync(name, new CircuitSnapshot(CircuitState.Closed, i, 0, 0, 0), 0, None))));

        Assert.Equal(1, results.Count(won => won));
        Assert.Equal(1, (await store.LoadAsync(name, None)).Version);
    }
}
