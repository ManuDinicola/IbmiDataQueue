namespace IbmiDataQueue;

public sealed class CircuitBreakerOptions
{
    /// <summary>Nombre d'échecs consécutifs qui ouvrent le disjoncteur.</summary>
    public int FailureThreshold { get; set; } = 5;

    /// <summary>Durée d'ouverture avant d'autoriser une sonde.</summary>
    public TimeSpan BreakDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Étalement aléatoire de la durée d'ouverture (0 à 1) : évite que toutes les instances sondent au même instant.</summary>
    public double Jitter { get; set; } = 0.2;

    /// <summary>Durée après laquelle une sonde sans résultat (instance tuée) est considérée perdue et remplaçable.</summary>
    public TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Durée pendant laquelle un état « fermé et sain » est mémorisé sans relire le store : évite plusieurs lectures
    /// en base par message quand tout va bien. Une ouverture décidée par une autre instance est vue au plus après ce délai.
    /// 0 = toujours relire.
    /// </summary>
    public TimeSpan HealthyCacheDuration { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Appelé quand le store du disjoncteur est en erreur. Le disjoncteur « échoue ouvert » (il laisse passer) :
    /// un store de disjoncteur en panne ne doit pas arrêter toute la consommation.
    /// </summary>
    public Action<Exception>? OnError { get; set; }
}
