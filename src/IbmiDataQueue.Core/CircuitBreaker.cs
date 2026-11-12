using System.Diagnostics;

namespace IbmiDataQueue;

/// <summary>
/// Disjoncteur à échecs consécutifs dont l'état vit dans un <see cref="ICircuitBreakerStore"/> (compare-and-swap).
/// Deux instances qui partagent le même store et le même nom partagent le même disjoncteur.
/// Si le store est en erreur, le disjoncteur laisse passer (voir <see cref="CircuitBreakerOptions.OnError"/>).
/// </summary>
public sealed class CircuitBreaker : ICircuitBreaker
{
    private const int MaxCasAttempts = 5;

    private readonly ICircuitBreakerStore _store;
    private readonly CircuitBreakerOptions _options;
    private readonly TimeProvider _time;
    private long _healthyUntilTicks;

    public CircuitBreaker(
        string name,
        ICircuitBreakerStore store,
        CircuitBreakerOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(store);

        _options = options ?? new CircuitBreakerOptions();
        if (_options.FailureThreshold < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "FailureThreshold doit valoir au moins 1.");
        }

        Name = name;
        _store = store;
        _time = timeProvider ?? TimeProvider.System;
    }

    public string Name { get; }

    private long Now => _time.GetUtcNow().UtcTicks;

    /// <summary>Vrai si l'état « fermé et sain » a été lu il y a moins de <see cref="CircuitBreakerOptions.HealthyCacheDuration"/>.</summary>
    private bool KnownHealthy =>
        _options.HealthyCacheDuration > TimeSpan.Zero && Now < Volatile.Read(ref _healthyUntilTicks);

    public async ValueTask<CircuitState> GetStateAsync(CancellationToken cancellationToken)
    {
        if (KnownHealthy)
        {
            return CircuitState.Closed;
        }

        try
        {
            return Effective(await LoadAsync(cancellationToken).ConfigureAwait(false), Now);
        }
        catch (Exception ex) when (IsStoreFailure(ex, cancellationToken))
        {
            _options.OnError?.Invoke(ex);
            return CircuitState.Closed;
        }
    }

    public async ValueTask<bool> TryAcquireAsync(CancellationToken cancellationToken)
    {
        if (KnownHealthy)
        {
            return true;
        }

        try
        {
            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var current = await LoadAsync(cancellationToken).ConfigureAwait(false);
                var now = Now;
                switch (Effective(current, now))
                {
                    case CircuitState.Closed:
                        return true;
                    case CircuitState.Open:
                        return false;
                    default:
                        // Une sonde est disponible : le compare-and-swap désigne un seul gagnant parmi les instances.
                        var probe = current with { State = CircuitState.HalfOpen, ProbeUntilTicks = now + _options.ProbeTimeout.Ticks };
                        if (await _store.TrySaveAsync(Name, probe, current.Version, cancellationToken).ConfigureAwait(false))
                        {
                            Volatile.Write(ref _healthyUntilTicks, 0);
                            Transition(CircuitState.HalfOpen);
                            return true;
                        }

                        break; // conflit : on relit l'état
                }
            }

            return false; // trop de contention : on réessaiera au prochain passage
        }
        catch (Exception ex) when (IsStoreFailure(ex, cancellationToken))
        {
            _options.OnError?.Invoke(ex);
            return true;
        }
    }

    public async ValueTask RecordSuccessAsync(CancellationToken cancellationToken)
    {
        if (KnownHealthy)
        {
            return;
        }

        try
        {
            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var current = await LoadAsync(cancellationToken).ConfigureAwait(false);
                if (current.State == CircuitState.Open || (current.State == CircuitState.Closed && current.ConsecutiveFailures == 0))
                {
                    return; // ouvert : seul le résultat d'une sonde peut refermer ; fermé et sain : rien à écrire (chemin chaud)
                }

                if (await _store.TrySaveAsync(Name, CircuitSnapshot.Initial, current.Version, cancellationToken).ConfigureAwait(false))
                {
                    MarkHealthy();
                    if (current.State != CircuitState.Closed)
                    {
                        Transition(CircuitState.Closed);
                    }

                    return;
                }
            }
        }
        catch (Exception ex) when (IsStoreFailure(ex, cancellationToken))
        {
            _options.OnError?.Invoke(ex);
        }
    }

    public async ValueTask RecordFailureAsync(CancellationToken cancellationToken)
    {
        try
        {
            for (var attempt = 0; attempt < MaxCasAttempts; attempt++)
            {
                var current = await LoadAsync(cancellationToken).ConfigureAwait(false);
                var now = Now;
                CircuitSnapshot next;
                switch (current.State)
                {
                    case CircuitState.Open:
                        return; // échec tardif d'une opération démarrée avant l'ouverture

                    case CircuitState.HalfOpen:
                        next = Opened(now, current.ConsecutiveFailures);
                        break;

                    default:
                        var failures = current.ConsecutiveFailures + 1;
                        next = failures >= _options.FailureThreshold
                            ? Opened(now, failures)
                            : current with { ConsecutiveFailures = failures };
                        break;
                }

                if (await _store.TrySaveAsync(Name, next, current.Version, cancellationToken).ConfigureAwait(false))
                {
                    Volatile.Write(ref _healthyUntilTicks, 0);
                    if (next.State != current.State)
                    {
                        Transition(next.State);
                    }

                    return;
                }
            }
        }
        catch (Exception ex) when (IsStoreFailure(ex, cancellationToken))
        {
            _options.OnError?.Invoke(ex);
        }
    }

    internal static CircuitState Effective(CircuitSnapshot snapshot, long nowTicks) => snapshot.State switch
    {
        CircuitState.Open => nowTicks >= snapshot.OpenUntilTicks ? CircuitState.HalfOpen : CircuitState.Open,
        CircuitState.HalfOpen => nowTicks >= snapshot.ProbeUntilTicks ? CircuitState.HalfOpen : CircuitState.Open,
        _ => CircuitState.Closed,
    };

    private static bool IsStoreFailure(Exception exception, CancellationToken cancellationToken) =>
        !(exception is OperationCanceledException && cancellationToken.IsCancellationRequested);

    private async ValueTask<CircuitSnapshot> LoadAsync(CancellationToken cancellationToken)
    {
        var snapshot = await _store.LoadAsync(Name, cancellationToken).ConfigureAwait(false);
        if (snapshot is { State: CircuitState.Closed, ConsecutiveFailures: 0 })
        {
            MarkHealthy();
        }
        else
        {
            Volatile.Write(ref _healthyUntilTicks, 0);
        }

        return snapshot;
    }

    private void MarkHealthy() =>
        Volatile.Write(ref _healthyUntilTicks, Now + _options.HealthyCacheDuration.Ticks);

    private CircuitSnapshot Opened(long now, int failures)
    {
        var jitter = Math.Clamp(_options.Jitter, 0d, 1d);
        var duration = _options.BreakDuration * (1d + (jitter * Random.Shared.NextDouble()));
        return new CircuitSnapshot(CircuitState.Open, failures, now + duration.Ticks, 0, 0);
    }

    private void Transition(CircuitState to) =>
        DataQueueTelemetry.CircuitTransitions.Add(
            1,
            new TagList { { "breaker", Name }, { "state", to.ToString() } });
}
