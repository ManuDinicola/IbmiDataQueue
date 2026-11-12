using System.Data.Common;
using System.Net.Sockets;

namespace IbmiDataQueue;

public sealed class PipelineOptions
{
    /// <summary>Attente maximale par lecture quand la queue est vide.</summary>
    public TimeSpan Wait { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Nombre d'échecs <b>permanents</b> du handler avant le dead-letter (1 = pas de retry).
    /// Les échecs transitoires (voir <see cref="IsTransient"/>) ne comptent pas : ils ne vont jamais directement au dead-letter.
    /// </summary>
    public int MaxHandlerAttempts { get; set; } = 3;

    /// <summary>Délai avant l'essai suivant, en fonction du nombre d'échecs consécutifs (1, 2...).</summary>
    public Func<int, TimeSpan> RetryBackoff { get; set; } = failures => TimeSpan.FromSeconds(Math.Pow(2, Math.Min(failures - 1, 16)));

    /// <summary>Plafond appliqué à <see cref="RetryBackoff"/>.</summary>
    public TimeSpan MaxRetryBackoff { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Pause de la boucle après une erreur (connexion perdue, dead-letter indisponible...).</summary>
    public TimeSpan ErrorDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Appelé pour chaque erreur rencontrée (échec d'un essai, erreur de lecture...). Brancher le logging ici.</summary>
    public Action<Exception>? OnError { get; set; }

    /// <summary>
    /// Disjoncteur de la connexion à la data queue. Ouvert : on ne lit plus.
    /// Les échecs de lecture alimentent ce disjoncteur.
    /// </summary>
    public ICircuitBreaker? ReceiveBreaker { get; set; }

    /// <summary>
    /// Disjoncteur de ce que le handler appelle (base, API...). Ouvert : on ne lit plus, les messages restent dans la queue IBM i.
    /// Les échecs transitoires du handler l'alimentent.
    /// </summary>
    public ICircuitBreaker? HandlerBreaker { get; set; }

    /// <summary>
    /// Une exception transitoire (dépendance indisponible, timeout) est rejouée sur le même message, jamais envoyée au dead-letter
    /// tant que <see cref="MaxTransientHold"/> n'est pas dépassé. Toute autre exception est permanente (message mauvais).
    /// </summary>
    public Func<Exception, bool> IsTransient { get; set; } = DefaultIsTransient;

    /// <summary>Durée maximale pendant laquelle un message reçu est conservé en attendant la dépendance, avant le dead-letter.</summary>
    public TimeSpan MaxTransientHold { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>Intervalle d'interrogation du disjoncteur quand il est ouvert.</summary>
    public TimeSpan CircuitOpenDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Nombre de boucles de lecture parallèles dans ce processus (utilisé par IbmiDataQueue.Hosting).
    /// Chaque boucle ne lit que lorsqu'elle est libre : au plus <c>Concurrency</c> messages en vol, aucune lecture anticipée.
    /// Chaque boucle garde une connexion ouverte pendant <see cref="Wait"/>.
    /// </summary>
    public int Concurrency { get; set; } = 1;

    /// <summary>Point de départ prudent : timeouts, erreurs réseau/E-S et exceptions ADO.NET marquées transitoires.</summary>
    public static bool DefaultIsTransient(Exception exception) => exception switch
    {
        TimeoutException or IOException or HttpRequestException or SocketException => true,
        DbException { IsTransient: true } => true,
        _ => false,
    };
}
