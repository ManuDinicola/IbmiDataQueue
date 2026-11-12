using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace IbmiDataQueue.SqlServer;

/// <summary>
/// Dead-letter dans une table SQL Server (voir <see cref="SqlServerSchema.DeadLetterTable"/>).
/// La connexion est fournie par l'appelant : la bibliothèque ne dépend d'aucun pilote.
/// </summary>
public sealed partial class SqlServerDeadLetterStore : IDeadLetterStore
{
    private readonly Func<CancellationToken, ValueTask<DbConnection>> _connect;
    private readonly SqlServerTable _table;
    private readonly TimeProvider _time;

    public SqlServerDeadLetterStore(
        Func<CancellationToken, ValueTask<DbConnection>> connect,
        SqlServerTable table,
        TimeProvider? timeProvider = null)
    {
        _connect = connect ?? throw new ArgumentNullException(nameof(connect));
        _table = table ?? throw new ArgumentNullException(nameof(table));
        _time = timeProvider ?? TimeProvider.System;
    }

    public async ValueTask WriteAsync(DeadLetter letter, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(letter);
        var now = _time.GetUtcNow();
        var sql = $"""
            INSERT INTO {_table.Quoted}
                (Id, ReceivedAtUtc, Library, QueueName, MessageKey, Reason, Attempts, ErrorType, ErrorMessage, ErrorDetail, Payload)
            VALUES
                (@id, @received, @library, @queue, @key, @reason, @attempts, @errorType, @errorMessage, @errorDetail, @payload);
            """;

        await using var connection = await _connect(cancellationToken).ConfigureAwait(false);
        await using var command = SqlServerTable.Command(
            connection,
            sql,
            ("@id", Guid.CreateVersion7(now).ToString("N")),
            ("@received", now.UtcDateTime),
            ("@library", letter.Queue.Library),
            ("@queue", letter.Queue.Name),
            ("@key", letter.Message.Key),
            ("@reason", letter.Reason.ToString()),
            ("@attempts", letter.Attempts),
            ("@errorType", letter.Error?.GetType().FullName ?? string.Empty),
            ("@errorMessage", letter.Error?.Message ?? string.Empty),
            ("@errorDetail", letter.Error?.ToString() ?? string.Empty),
            ("@payload", letter.Message.Payload.ToArray()));

        // CancellationToken.None : une écriture de dead-letter ne doit pas être abandonnée à l'arrêt du service.
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    public async IAsyncEnumerable<StoredDeadLetter> ListAsync(int max, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (max <= 0)
        {
            yield break;
        }

        var sql = $"""
            SELECT TOP (@max) Id, ReceivedAtUtc, Library, QueueName, MessageKey, Reason, Attempts, ErrorType, ErrorMessage, ErrorDetail, Payload
            FROM {_table.Quoted}
            ORDER BY ReceivedAtUtc, Id;
            """;

        await using var connection = await _connect(cancellationToken).ConfigureAwait(false);
        await using var command = SqlServerTable.Command(connection, sql, ("@max", max));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return new StoredDeadLetter(
                reader.GetString(0).TrimEnd(),
                new DateTimeOffset(DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc)),
                new QueueRef(reader.GetString(2), reader.GetString(3)),
                (byte[])reader.GetValue(10),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                Enum.Parse<DeadLetterReason>(reader.GetString(5)),
                reader.GetInt32(6),
                reader.GetString(7),
                reader.GetString(8),
                reader.GetString(9));
        }
    }

    public async ValueTask<bool> DeleteAsync(string id, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(id) || !IdPattern().IsMatch(id))
        {
            return false;
        }

        await using var connection = await _connect(cancellationToken).ConfigureAwait(false);
        await using var command = SqlServerTable.Command(connection, $"DELETE FROM {_table.Quoted} WHERE Id = @id;", ("@id", id));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    [GeneratedRegex("^[0-9a-f]{32}$")]
    private static partial Regex IdPattern();
}
