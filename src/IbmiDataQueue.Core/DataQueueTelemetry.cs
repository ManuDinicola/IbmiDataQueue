using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace IbmiDataQueue;

/// <summary>
/// Sources de télémétrie compatibles OpenTelemetry :
/// <c>AddSource(DataQueueTelemetry.Name)</c> et <c>AddMeter(DataQueueTelemetry.Name)</c>.
/// </summary>
public static class DataQueueTelemetry
{
    public const string Name = "IbmiDataQueue";

    public static readonly ActivitySource ActivitySource = new(Name);

    private static readonly Meter Meter = new(Name);

    internal static readonly Counter<long> Received =
        Meter.CreateCounter<long>("ibmi.dataqueue.messages.received", "{message}", "Messages reçus de la data queue.");

    internal static readonly Counter<long> Handled =
        Meter.CreateCounter<long>("ibmi.dataqueue.messages.handled", "{message}", "Messages traités avec succès.");

    internal static readonly Counter<long> DeadLettered =
        Meter.CreateCounter<long>("ibmi.dataqueue.messages.dead_lettered", "{message}", "Messages envoyés au dead-letter.");

    internal static readonly Counter<long> Errors =
        Meter.CreateCounter<long>("ibmi.dataqueue.errors", "{error}", "Erreurs de la boucle de consommation.");

    internal static readonly Counter<long> CircuitTransitions =
        Meter.CreateCounter<long>("ibmi.dataqueue.circuit.transitions", "{transition}", "Changements d'état des disjoncteurs.");

    internal static readonly Counter<long> CircuitRejections =
        Meter.CreateCounter<long>("ibmi.dataqueue.circuit.rejections", "{rejection}", "Lectures évitées parce qu'un disjoncteur est ouvert.");
}
