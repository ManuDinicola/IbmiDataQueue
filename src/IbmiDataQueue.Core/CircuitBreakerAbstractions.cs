namespace IbmiDataQueue;

public enum CircuitState : byte
{
    /// <summary>Tout passe.</summary>
    Closed = 0,

    /// <summary>Dépendance considérée en panne : on ne lit plus la queue, les messages y restent en sécurité.</summary>
    Open = 1,

    /// <summary>La pause est terminée : une seule opération « sonde » est autorisée pour tester la dépendance.</summary>
    HalfOpen = 2,
}

/// <summary>
/// État persistant d'un disjoncteur. Les dates sont des ticks UTC (<see cref="DateTimeOffset.UtcTicks"/>),
/// portables vers une colonne bigint ou un fichier JSON. <see cref="Version"/> sert au compare-and-swap du store.
/// </summary>
public sealed record CircuitSnapshot(
    CircuitState State,
    int ConsecutiveFailures,
    long OpenUntilTicks,
    long ProbeUntilTicks,
    long Version)
{
    /// <summary>État d'un disjoncteur jamais utilisé (version 0 = ligne absente du store).</summary>
    public static CircuitSnapshot Initial { get; } = new(CircuitState.Closed, 0, 0, 0, 0);
}

/// <summary>
/// Où l'état d'un disjoncteur est conservé. En mémoire : un état par instance. Dans un store partagé
/// (SQL Server, fichier sur volume commun...) : toutes les instances s'arrêtent et reprennent ensemble.
/// </summary>
public interface ICircuitBreakerStore
{
    /// <summary>Retourne l'état courant, ou <see cref="CircuitSnapshot.Initial"/> si le disjoncteur n'existe pas encore.</summary>
    ValueTask<CircuitSnapshot> LoadAsync(string name, CancellationToken cancellationToken);

    /// <summary>
    /// Compare-and-swap : enregistre <paramref name="next"/> (avec la version <c>expectedVersion + 1</c>) seulement si la version
    /// stockée vaut encore <paramref name="expectedVersion"/> (0 = absent). Retourne false en cas de conflit.
    /// </summary>
    ValueTask<bool> TrySaveAsync(string name, CircuitSnapshot next, long expectedVersion, CancellationToken cancellationToken);
}

/// <summary>
/// Disjoncteur. Chaque <see cref="TryAcquireAsync"/> réussi doit être suivi d'exactement un
/// <see cref="RecordSuccessAsync"/> ou <see cref="RecordFailureAsync"/>.
/// </summary>
public interface ICircuitBreaker
{
    string Name { get; }

    /// <summary>
    /// État effectif, sans rien réserver. <see cref="CircuitState.HalfOpen"/> signifie « une sonde est disponible » ;
    /// <see cref="CircuitState.Open"/> couvre aussi « sonde déjà en cours ailleurs ».
    /// </summary>
    ValueTask<CircuitState> GetStateAsync(CancellationToken cancellationToken);

    /// <summary>Demande l'autorisation d'une opération. False = ne pas l'exécuter. En demi-ouvert, un seul appelant obtient la sonde.</summary>
    ValueTask<bool> TryAcquireAsync(CancellationToken cancellationToken);

    ValueTask RecordSuccessAsync(CancellationToken cancellationToken);

    ValueTask RecordFailureAsync(CancellationToken cancellationToken);
}
