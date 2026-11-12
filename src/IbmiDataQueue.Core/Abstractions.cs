namespace IbmiDataQueue;

/// <summary>Lit une data queue. Implémentations : SQL (QSYS2.RECEIVE_DATA_QUEUE), en mémoire pour les tests.</summary>
public interface IDataQueueClient
{
    /// <summary>
    /// Reçoit le prochain message, en attendant au plus <paramref name="wait"/>.
    /// Retourne null si la queue reste vide. Le message est retiré de la queue.
    /// </summary>
    ValueTask<DataQueueMessage?> ReceiveAsync(QueueRef queue, TimeSpan wait, CancellationToken cancellationToken);
}

/// <summary>Transforme les octets d'un message en objet. Toute exception levée = message empoisonné (dead-letter).</summary>
public interface IMessageCodec<out T>
{
    T Decode(ReadOnlySpan<byte> payload);
}

/// <summary>Traitement métier d'un message décodé. Toute exception levée déclenche le retry puis le dead-letter.</summary>
public interface IMessageHandler<in T>
{
    ValueTask HandleAsync(T message, CancellationToken cancellationToken);
}

/// <summary>Destination des messages qu'on n'a pas pu traiter. Doit être durable : le message n'existe plus dans la queue.</summary>
public interface IDeadLetterSink
{
    ValueTask WriteAsync(DeadLetter letter, CancellationToken cancellationToken);
}

public enum DeadLetterReason
{
    /// <summary>Le codec a refusé le message (longueur, format...). Pas de retry.</summary>
    DecodeFailed,

    /// <summary>Le handler a échoué après tous les essais.</summary>
    HandlerFailed,

    /// <summary>Arrêt demandé pendant le traitement : le message est conservé pour ne pas être perdu.</summary>
    Interrupted,
}

public sealed record DeadLetter(
    QueueRef Queue,
    DataQueueMessage Message,
    DeadLetterReason Reason,
    Exception Error,
    int Attempts);
