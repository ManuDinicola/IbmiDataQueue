using Microsoft.Extensions.Hosting;

namespace IbmiDataQueue.Hosting;

public sealed class DataQueueWorker<T> : BackgroundService
{
    private readonly DataQueuePipeline<T> _pipeline;

    public DataQueueWorker(DataQueuePipeline<T> pipeline)
    {
        _pipeline = pipeline;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Rend la main tout de suite : le démarrage de l'hôte ne doit pas attendre la première lecture.
        await Task.Yield();
        // Concurrency boucles de lecture : chacune ne lit que lorsqu'elle est libre (pas de prélecture).
        // Chaque boucle garde une connexion ouverte pendant l'attente : budget de connexions = instances x Concurrency.
        var loops = Enumerable.Range(0, _pipeline.Concurrency).Select(_ => _pipeline.RunAsync(stoppingToken));
        await Task.WhenAll(loops).ConfigureAwait(false);
    }
}
