using System.Diagnostics;

namespace IbmiDataQueue;

public enum ProcessResult
{
    NoMessage,
    Handled,
    DeadLettered,

    /// <summary>Un disjoncteur est ouvert : rien n'a été lu, les messages restent dans la queue.</summary>
    CircuitOpen,
}

/// <summary>
/// Recevoir → décoder → traiter. Un message reçu est toujours soit traité, soit écrit dans le dead-letter
/// (livraison « au plus une fois » côté queue, rendue sûre par le dead-letter). Si le dead-letter échoue,
/// l'exception remonte : le message est alors perdu, mais l'erreur est visible.
/// <para>
/// Résilience : un disjoncteur ouvert empêche de lire, donc une panne en aval ne vide pas la queue. Une erreur transitoire du handler
/// n'envoie jamais le message au dead-letter : il est conservé et rejoué jusqu'au retour de la dépendance (ou jusqu'à
/// <see cref="PipelineOptions.MaxTransientHold"/>). Seules les erreurs permanentes (message mauvais) y vont après leurs essais.
/// </para>
/// </summary>
public sealed class DataQueuePipeline<T>
{
    private readonly IDataQueueClient _client;
    private readonly QueueRef _queue;
    private readonly IMessageCodec<T> _codec;
    private readonly IMessageHandler<T> _handler;
    private readonly IDeadLetterSink _deadLetters;
    private readonly PipelineOptions _options;
    private readonly TimeProvider _time;
    private readonly KeyValuePair<string, object?> _queueTag;

    public DataQueuePipeline(
        IDataQueueClient client,
        QueueRef queue,
        IMessageCodec<T> codec,
        IMessageHandler<T> handler,
        IDeadLetterSink deadLetters,
        PipelineOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(codec);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(deadLetters);

        _options = options ?? new PipelineOptions();
        if (_options.MaxHandlerAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MaxHandlerAttempts doit valoir au moins 1.");
        }

        if (_options.Concurrency < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Concurrency doit valoir au moins 1.");
        }

        _client = client;
        _queue = queue;
        _codec = codec;
        _handler = handler;
        _deadLetters = deadLetters;
        _time = timeProvider ?? TimeProvider.System;
        _queueTag = new KeyValuePair<string, object?>("queue", queue.ToString());
    }

    /// <summary>Nombre de boucles <see cref="RunAsync"/> à lancer en parallèle dans ce processus.</summary>
    public int Concurrency => _options.Concurrency;

    /// <summary>Traite au plus un message.</summary>
    /// <exception cref="OperationCanceledException">Arrêt demandé. Un message en cours de traitement est d'abord écrit au dead-letter.</exception>
    public async ValueTask<ProcessResult> ProcessNextAsync(CancellationToken cancellationToken)
    {
        var handlerBreaker = _options.HandlerBreaker;
        var receiveBreaker = _options.ReceiveBreaker;

        // Ne rien lire tant qu'une dépendance est connue pour être en panne : les messages restent dans la queue IBM i.
        if (handlerBreaker is not null
            && await handlerBreaker.GetStateAsync(cancellationToken).ConfigureAwait(false) == CircuitState.Open)
        {
            return Rejected();
        }

        if (receiveBreaker is not null
            && !await receiveBreaker.TryAcquireAsync(cancellationToken).ConfigureAwait(false))
        {
            return Rejected();
        }

        DataQueueMessage? message;
        try
        {
            message = await _client.ReceiveAsync(_queue, _options.Wait, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!IsShutdown(ex, cancellationToken))
        {
            if (receiveBreaker is not null)
            {
                await receiveBreaker.RecordFailureAsync(cancellationToken).ConfigureAwait(false);
            }

            throw;
        }

        if (receiveBreaker is not null)
        {
            await receiveBreaker.RecordSuccessAsync(cancellationToken).ConfigureAwait(false);
        }

        if (message is null)
        {
            return ProcessResult.NoMessage;
        }

        using var activity = DataQueueTelemetry.ActivitySource.StartActivity("dataqueue.process");
        activity?.SetTag("ibmi.queue", _queue.ToString());
        DataQueueTelemetry.Received.Add(1, _queueTag);

        T decoded;
        try
        {
            decoded = _codec.Decode(message.Payload.Span);
        }
        catch (Exception ex)
        {
            return await DeadLetterAsync(message, DeadLetterReason.DecodeFailed, ex, attempts: 0).ConfigureAwait(false);
        }

        var invocations = 0;
        var permanentFailures = 0;
        var transientFailures = 0;
        Exception? lastError = null;
        var heldSince = _time.GetTimestamp();
        try
        {
            while (true)
            {
                if (handlerBreaker is not null)
                {
                    // Le message est déjà sorti de la queue : on le garde et on attend l'autorisation (sonde) du disjoncteur.
                    while (!await handlerBreaker.TryAcquireAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if (HoldExceeded(heldSince))
                        {
                            var openError = lastError ?? new TimeoutException(
                                $"Disjoncteur « {handlerBreaker.Name} » ouvert depuis plus de {_options.MaxTransientHold}.");
                            return await DeadLetterAsync(message, DeadLetterReason.HandlerFailed, openError, invocations).ConfigureAwait(false);
                        }

                        await Task.Delay(_options.CircuitOpenDelay, _time, cancellationToken).ConfigureAwait(false);
                    }
                }

                invocations++;
                try
                {
                    await _handler.HandleAsync(decoded, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (!IsShutdown(ex, cancellationToken))
                {
                    lastError = ex;
                    _options.OnError?.Invoke(ex);

                    if (_options.IsTransient(ex))
                    {
                        transientFailures++;
                        if (handlerBreaker is not null)
                        {
                            await handlerBreaker.RecordFailureAsync(cancellationToken).ConfigureAwait(false);
                        }

                        if (HoldExceeded(heldSince))
                        {
                            return await DeadLetterAsync(message, DeadLetterReason.HandlerFailed, ex, invocations).ConfigureAwait(false);
                        }

                        await Task.Delay(Backoff(transientFailures), _time, cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    // Erreur permanente : la dépendance a répondu, c'est le message qui est mauvais. On résout la sonde éventuelle.
                    if (handlerBreaker is not null)
                    {
                        await handlerBreaker.RecordSuccessAsync(cancellationToken).ConfigureAwait(false);
                    }

                    permanentFailures++;
                    if (permanentFailures >= _options.MaxHandlerAttempts)
                    {
                        return await DeadLetterAsync(message, DeadLetterReason.HandlerFailed, ex, invocations).ConfigureAwait(false);
                    }

                    await Task.Delay(Backoff(permanentFailures), _time, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (handlerBreaker is not null)
                {
                    await handlerBreaker.RecordSuccessAsync(cancellationToken).ConfigureAwait(false);
                }

                DataQueueTelemetry.Handled.Add(1, _queueTag);
                return ProcessResult.Handled;
            }
        }
        catch (Exception ex) when (IsShutdown(ex, cancellationToken))
        {
            // Le message n'est plus dans la queue : on le conserve avant de laisser l'arrêt se poursuivre.
            await DeadLetterAsync(message, DeadLetterReason.Interrupted, ex, invocations).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Boucle de consommation jusqu'à l'annulation. Les erreurs sont rapportées puis la boucle continue.</summary>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await ProcessNextAsync(cancellationToken).ConfigureAwait(false);
                if (result == ProcessResult.CircuitOpen)
                {
                    await Task.Delay(_options.CircuitOpenDelay, _time, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (IsShutdown(ex, cancellationToken))
            {
                break;
            }
            catch (Exception ex)
            {
                DataQueueTelemetry.Errors.Add(1, _queueTag);
                _options.OnError?.Invoke(ex);
                try
                {
                    await Task.Delay(_options.ErrorDelay, _time, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }

    private static bool IsShutdown(Exception exception, CancellationToken cancellationToken) =>
        exception is OperationCanceledException && cancellationToken.IsCancellationRequested;

    private ProcessResult Rejected()
    {
        DataQueueTelemetry.CircuitRejections.Add(1, _queueTag);
        return ProcessResult.CircuitOpen;
    }

    private bool HoldExceeded(long heldSince) => _time.GetElapsedTime(heldSince) >= _options.MaxTransientHold;

    private TimeSpan Backoff(int failures)
    {
        var delay = _options.RetryBackoff(failures);
        return delay > _options.MaxRetryBackoff ? _options.MaxRetryBackoff : delay;
    }

    private async ValueTask<ProcessResult> DeadLetterAsync(DataQueueMessage message, DeadLetterReason reason, Exception error, int attempts)
    {
        // CancellationToken.None : même pendant l'arrêt, on ne veut pas perdre le message.
        await _deadLetters
            .WriteAsync(new DeadLetter(_queue, message, reason, error, attempts), CancellationToken.None)
            .ConfigureAwait(false);

        DataQueueTelemetry.DeadLettered.Add(1, new TagList { _queueTag, { "reason", reason.ToString() } });
        return ProcessResult.DeadLettered;
    }
}
