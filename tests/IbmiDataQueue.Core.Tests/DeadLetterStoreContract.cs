using IbmiDataQueue.Testing;
using Xunit;

namespace IbmiDataQueue.Core.Tests;

/// <summary>
/// Contrat commun à tous les <see cref="IDeadLetterStore"/> (fichiers, SQL Server...).
/// Une implémentation l'hérite et fournit <see cref="CreateStore"/>.
/// </summary>
public abstract class DeadLetterStoreContract
{
    private static readonly CancellationToken None = CancellationToken.None;

    protected readonly ManualTimeProvider Time = new(new DateTimeOffset(2026, 11, 12, 10, 0, 0, TimeSpan.Zero));

    protected abstract IDeadLetterStore CreateStore(TimeProvider time);

    protected static QueueRef UniqueQueue() => new("TESTLIB", "Q" + Guid.NewGuid().ToString("N")[..8]);

    private static DeadLetter Letter(QueueRef queue, byte[] payload, string? key = null, int attempts = 3) =>
        new(queue, new DataQueueMessage(payload, key), DeadLetterReason.HandlerFailed, new InvalidOperationException("boom"), attempts);

    private static async Task<List<StoredDeadLetter>> ListAsync(IDeadLetterStore store, int max = 100)
    {
        var items = new List<StoredDeadLetter>();
        await foreach (var item in store.ListAsync(max, None))
        {
            items.Add(item);
        }

        return items;
    }

    [Fact]
    public async Task A_written_letter_is_listed_with_all_its_fields()
    {
        var store = CreateStore(Time);
        var queue = UniqueQueue();
        await store.WriteAsync(Letter(queue, [1, 2, 3], key: "K1"), None);

        var stored = (await ListAsync(store)).Single(item => item.Queue == queue);

        Assert.Equal(queue, stored.Queue);
        Assert.Equal(new byte[] { 1, 2, 3 }, stored.Payload.ToArray());
        Assert.Equal("K1", stored.Key);
        Assert.Equal(DeadLetterReason.HandlerFailed, stored.Reason);
        Assert.Equal(3, stored.Attempts);
        Assert.Equal(typeof(InvalidOperationException).FullName, stored.ErrorType);
        Assert.Equal("boom", stored.ErrorMessage);
        Assert.Contains("boom", stored.ErrorDetail);
        Assert.Equal(Time.GetUtcNow(), stored.ReceivedAtUtc);
        Assert.True(stored.Id.Length == 32, "Id attendu : 32 caractères hexadécimaux.");
    }

    [Fact]
    public async Task A_null_key_stays_null()
    {
        var store = CreateStore(Time);
        var queue = UniqueQueue();
        await store.WriteAsync(Letter(queue, [9]), None);

        Assert.Null((await ListAsync(store)).Single(item => item.Queue == queue).Key);
    }

    [Fact]
    public async Task The_binary_payload_is_preserved_exactly()
    {
        var store = CreateStore(Time);
        var queue = UniqueQueue();
        var payload = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
        await store.WriteAsync(Letter(queue, payload), None);

        Assert.Equal(payload, (await ListAsync(store)).Single(item => item.Queue == queue).Payload.ToArray());
    }

    [Fact]
    public async Task An_empty_payload_is_supported()
    {
        var store = CreateStore(Time);
        var queue = UniqueQueue();
        await store.WriteAsync(Letter(queue, []), None);

        Assert.Equal(0, (await ListAsync(store)).Single(item => item.Queue == queue).Payload.Length);
    }

    [Fact]
    public async Task Letters_are_listed_oldest_first_and_the_list_respects_max()
    {
        var store = CreateStore(Time);
        var queue = UniqueQueue();
        for (var i = 0; i < 3; i++)
        {
            await store.WriteAsync(Letter(queue, [(byte)i]), None);
            Time.Advance(TimeSpan.FromSeconds(1));
        }

        var all = (await ListAsync(store)).Where(item => item.Queue == queue).ToList();
        var firstTwo = (await ListAsync(store, max: 2)).Count;

        Assert.Equal(new byte[] { 0, 1, 2 }, all.Select(item => item.Payload.Span[0]).ToArray());
        Assert.True(firstTwo <= 2, "max doit limiter le nombre de lignes.");
    }

    [Fact]
    public async Task Delete_removes_a_letter_and_reports_whether_it_existed()
    {
        var store = CreateStore(Time);
        var queue = UniqueQueue();
        await store.WriteAsync(Letter(queue, [1]), None);
        var id = (await ListAsync(store)).Single(item => item.Queue == queue).Id;

        Assert.True(await store.DeleteAsync(id, None));
        Assert.True(!await store.DeleteAsync(id, None));
        Assert.Empty((await ListAsync(store)).Where(item => item.Queue == queue));
    }

    [Fact]
    public async Task Delete_of_an_unknown_or_malformed_id_returns_false()
    {
        var store = CreateStore(Time);

        Assert.True(!await store.DeleteAsync("00000000000000000000000000000000", None));
        Assert.True(!await store.DeleteAsync("../../etc/passwd", None));
        Assert.True(!await store.DeleteAsync("'; DROP TABLE x;--", None));
        Assert.True(!await store.DeleteAsync(string.Empty, None));
    }
}
