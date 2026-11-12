using System.Data.Common;

namespace IbmiDataQueue.SqlServer;

/// <summary>
/// État du disjoncteur dans une table SQL Server (voir <see cref="SqlServerSchema.CircuitBreakerTable"/>).
/// Compare-and-swap : <c>UPDATE ... WHERE Version = @expected</c>, ou insertion unique quand la version attendue est 0.
/// Toutes les instances qui pointent vers la même table et le même nom partagent le même disjoncteur.
/// </summary>
public sealed class SqlServerCircuitBreakerStore : ICircuitBreakerStore
{
    private const int MaxNameLength = 200;

    private readonly Func<CancellationToken, ValueTask<DbConnection>> _connect;
    private readonly SqlServerTable _table;

    public SqlServerCircuitBreakerStore(Func<CancellationToken, ValueTask<DbConnection>> connect, SqlServerTable table)
    {
        _connect = connect ?? throw new ArgumentNullException(nameof(connect));
        _table = table ?? throw new ArgumentNullException(nameof(table));
    }

    public async ValueTask<CircuitSnapshot> LoadAsync(string name, CancellationToken cancellationToken)
    {
        CheckName(name);
        var sql = $"SELECT State, ConsecutiveFailures, OpenUntilTicks, ProbeUntilTicks, Version FROM {_table.Quoted} WHERE Name = @name;";

        await using var connection = await _connect(cancellationToken).ConfigureAwait(false);
        await using var command = SqlServerTable.Command(connection, sql, ("@name", name));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return CircuitSnapshot.Initial;
        }

        return new CircuitSnapshot(
            (CircuitState)reader.GetByte(0),
            reader.GetInt32(1),
            reader.GetInt64(2),
            reader.GetInt64(3),
            reader.GetInt64(4));
    }

    public async ValueTask<bool> TrySaveAsync(string name, CircuitSnapshot next, long expectedVersion, CancellationToken cancellationToken)
    {
        CheckName(name);
        ArgumentNullException.ThrowIfNull(next);

        var sql = expectedVersion == 0
            ? $"""
                INSERT INTO {_table.Quoted} (Name, State, ConsecutiveFailures, OpenUntilTicks, ProbeUntilTicks, Version)
                SELECT @name, @state, @failures, @openUntil, @probeUntil, 1
                WHERE NOT EXISTS (SELECT 1 FROM {_table.Quoted} WITH (UPDLOCK, HOLDLOCK) WHERE Name = @name);
                """
            : $"""
                UPDATE {_table.Quoted}
                SET State = @state, ConsecutiveFailures = @failures, OpenUntilTicks = @openUntil, ProbeUntilTicks = @probeUntil, Version = @expected + 1
                WHERE Name = @name AND Version = @expected;
                """;

        await using var connection = await _connect(cancellationToken).ConfigureAwait(false);
        await using var command = SqlServerTable.Command(
            connection,
            sql,
            ("@name", name),
            ("@state", (byte)next.State),
            ("@failures", next.ConsecutiveFailures),
            ("@openUntil", next.OpenUntilTicks),
            ("@probeUntil", next.ProbeUntilTicks),
            ("@expected", expectedVersion));

        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) == 1;
    }

    private static void CheckName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (name.Length > MaxNameLength)
        {
            throw new ArgumentException($"Le nom du disjoncteur est limité à {MaxNameLength} caractères.", nameof(name));
        }
    }
}
