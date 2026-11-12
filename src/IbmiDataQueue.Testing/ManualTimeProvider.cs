namespace IbmiDataQueue.Testing;

/// <summary>
/// Horloge pilotée par le test : <see cref="GetUtcNow"/> et <see cref="TimeProvider.GetTimestamp"/> n'avancent que
/// par <see cref="Advance"/>. Les minuteurs (Task.Delay) restent ceux du système : garder des délais très courts dans les tests.
/// </summary>
public sealed class ManualTimeProvider : TimeProvider
{
    private long _ticks;

    public ManualTimeProvider(DateTimeOffset? start = null)
    {
        _ticks = (start ?? new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)).UtcTicks;
    }

    public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

    public override long GetTimestamp() => Interlocked.Read(ref _ticks);

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
}
