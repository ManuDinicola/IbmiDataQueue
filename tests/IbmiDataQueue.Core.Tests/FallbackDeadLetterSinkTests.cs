using IbmiDataQueue.Testing;
using Xunit;

namespace IbmiDataQueue.Core.Tests;

public class FallbackDeadLetterSinkTests
{
    private static readonly DeadLetter Letter = new(
        new QueueRef("TESTLIB", "ECHOQ"),
        new DataQueueMessage(new byte[] { 1, 2, 3 }),
        DeadLetterReason.DecodeFailed,
        new FormatException("x"),
        0);

    [Fact]
    public async Task Uses_only_the_primary_when_it_works()
    {
        var primary = new InMemoryDeadLetterSink();
        var fallback = new InMemoryDeadLetterSink();

        await new FallbackDeadLetterSink(primary, fallback).WriteAsync(Letter, CancellationToken.None);

        Assert.Single(primary.Items);
        Assert.Empty(fallback.Items);
    }

    [Fact]
    public async Task Falls_back_when_the_primary_fails_and_reports_the_failure()
    {
        var fallback = new InMemoryDeadLetterSink();
        var failures = new List<Exception>();

        await new FallbackDeadLetterSink(new ThrowingSink("primaire"), fallback, failures.Add)
            .WriteAsync(Letter, CancellationToken.None);

        Assert.Single(fallback.Items);
        Assert.Equal("primaire", Assert.Single(failures).Message);
    }

    [Fact]
    public async Task Throws_both_errors_when_both_sinks_fail()
    {
        var sink = new FallbackDeadLetterSink(new ThrowingSink("primaire"), new ThrowingSink("repli"));

        var error = await Assert.ThrowsAsync<AggregateException>(() => sink.WriteAsync(Letter, CancellationToken.None).AsTask());

        Assert.Equal(2, error.InnerExceptions.Count);
    }

    private sealed class ThrowingSink(string message) : IDeadLetterSink
    {
        public ValueTask WriteAsync(DeadLetter letter, CancellationToken cancellationToken) =>
            throw new InvalidOperationException(message);
    }
}
