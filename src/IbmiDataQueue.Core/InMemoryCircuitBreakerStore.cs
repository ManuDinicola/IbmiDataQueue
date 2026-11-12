namespace IbmiDataQueue;

/// <summary>Store par défaut : l'état reste dans le processus, donc chaque instance a son propre disjoncteur.</summary>
public sealed class InMemoryCircuitBreakerStore : ICircuitBreakerStore
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, CircuitSnapshot> _items = [];

    public ValueTask<CircuitSnapshot> LoadAsync(string name, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            return ValueTask.FromResult(_items.GetValueOrDefault(name) ?? CircuitSnapshot.Initial);
        }
    }

    public ValueTask<bool> TrySaveAsync(string name, CircuitSnapshot next, long expectedVersion, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var current = _items.GetValueOrDefault(name) ?? CircuitSnapshot.Initial;
            if (current.Version != expectedVersion)
            {
                return ValueTask.FromResult(false);
            }

            _items[name] = next with { Version = expectedVersion + 1 };
            return ValueTask.FromResult(true);
        }
    }
}
