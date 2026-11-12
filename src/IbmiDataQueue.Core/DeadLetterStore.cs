namespace IbmiDataQueue;

/// <summary>Un message du dead-letter tel que relu depuis un store (pour inspection ou rejeu).</summary>
public sealed record StoredDeadLetter(
    string Id,
    DateTimeOffset ReceivedAtUtc,
    QueueRef Queue,
    ReadOnlyMemory<byte> Payload,
    string? Key,
    DeadLetterReason Reason,
    int Attempts,
    string ErrorType,
    string ErrorMessage,
    string ErrorDetail);

/// <summary>
/// Dead-letter qu'on peut aussi relire : sans cela, un message en erreur serait durable mais inexploitable.
/// Implémentations fournies : système de fichiers (IbmiDataQueue.FileSystem) et SQL Server (IbmiDataQueue.SqlServer).
/// </summary>
public interface IDeadLetterStore : IDeadLetterSink
{
    /// <summary>Les plus anciens d'abord, au plus <paramref name="max"/>.</summary>
    IAsyncEnumerable<StoredDeadLetter> ListAsync(int max, CancellationToken cancellationToken);

    /// <summary>Supprime un message (après rejeu ou abandon). False s'il n'existe pas.</summary>
    ValueTask<bool> DeleteAsync(string id, CancellationToken cancellationToken);
}

/// <summary>
/// Écrit dans le dead-letter principal ; s'il est indisponible, écrit dans celui de repli (par exemple fichier local).
/// Si les deux échouent, l'exception remonte (message perdu, mais l'erreur est visible).
/// </summary>
public sealed class FallbackDeadLetterSink : IDeadLetterSink
{
    private readonly IDeadLetterSink _primary;
    private readonly IDeadLetterSink _fallback;
    private readonly Action<Exception>? _onPrimaryFailure;

    public FallbackDeadLetterSink(IDeadLetterSink primary, IDeadLetterSink fallback, Action<Exception>? onPrimaryFailure = null)
    {
        _primary = primary ?? throw new ArgumentNullException(nameof(primary));
        _fallback = fallback ?? throw new ArgumentNullException(nameof(fallback));
        _onPrimaryFailure = onPrimaryFailure;
    }

    public async ValueTask WriteAsync(DeadLetter letter, CancellationToken cancellationToken)
    {
        try
        {
            await _primary.WriteAsync(letter, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception primaryError)
        {
            _onPrimaryFailure?.Invoke(primaryError);
            try
            {
                await _fallback.WriteAsync(letter, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception fallbackError)
            {
                throw new AggregateException(
                    "Dead-letter principal et dead-letter de repli indisponibles.",
                    primaryError,
                    fallbackError);
            }
        }
    }
}
