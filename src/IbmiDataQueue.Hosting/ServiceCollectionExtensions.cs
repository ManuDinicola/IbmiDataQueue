using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace IbmiDataQueue.Hosting;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Enregistre un consommateur de data queue : codec, handler (singletons) et BackgroundService.
    /// Prérequis : enregistrer <see cref="IDataQueueClient"/> et <see cref="IDeadLetterSink"/>.
    /// </summary>
    public static IServiceCollection AddDataQueueConsumer<T, TCodec, THandler>(
        this IServiceCollection services,
        QueueRef queue,
        Action<PipelineOptions>? configure = null)
        where TCodec : class, IMessageCodec<T>
        where THandler : class, IMessageHandler<T> =>
        services.AddDataQueueConsumer<T, TCodec, THandler>(queue, configure is null ? null : (_, options) => configure(options));

    /// <summary>
    /// Variante qui donne accès au conteneur : permet de construire les disjoncteurs (<see cref="CircuitBreaker"/>)
    /// et le dead-letter à partir de services enregistrés (store SQL Server, fichiers...).
    /// </summary>
    public static IServiceCollection AddDataQueueConsumer<T, TCodec, THandler>(
        this IServiceCollection services,
        QueueRef queue,
        Action<IServiceProvider, PipelineOptions>? configure)
        where TCodec : class, IMessageCodec<T>
        where THandler : class, IMessageHandler<T>
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IMessageCodec<T>, TCodec>();
        services.AddSingleton<IMessageHandler<T>, THandler>();
        services.AddSingleton(provider =>
        {
            var options = new PipelineOptions();
            configure?.Invoke(provider, options);

            var logger = provider.GetRequiredService<ILoggerFactory>().CreateLogger($"IbmiDataQueue.{queue}");
            options.OnError ??= error => logger.LogError(error, "Erreur de traitement sur la data queue {Queue}.", queue);

            return new DataQueuePipeline<T>(
                provider.GetRequiredService<IDataQueueClient>(),
                queue,
                provider.GetRequiredService<IMessageCodec<T>>(),
                provider.GetRequiredService<IMessageHandler<T>>(),
                provider.GetRequiredService<IDeadLetterSink>(),
                options,
                provider.GetService<TimeProvider>());
        });
        services.AddHostedService<DataQueueWorker<T>>();

        return services;
    }
}
