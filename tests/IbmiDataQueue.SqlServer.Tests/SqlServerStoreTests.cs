using System.Reflection;
using IbmiDataQueue.Core.Tests;
using Xunit;

namespace IbmiDataQueue.SqlServer.Tests;

/// <summary>
/// Exécute les contrats communs (les mêmes que pour la mémoire et les fichiers) contre une vraie base SQL Server.
/// Chaque méthode du contrat est lancée sur une instance neuve ; l'échec indique laquelle.
/// </summary>
public class SqlServerStoreTests
{
    private static readonly SqlServerTable DeadLetterTable = new("DqTestDeadLetter");
    private static readonly SqlServerTable BreakerTable = new("DqTestCircuitBreaker");

    [SqlServerFact]
    [Trait("Category", "Integration")]
    public async Task Circuit_breaker_store_honours_the_shared_contract() =>
        await RunContractAsync<SqlServerCircuitBreakerStoreContract>();

    [SqlServerFact]
    [Trait("Category", "Integration")]
    public async Task Dead_letter_store_honours_the_shared_contract() =>
        await RunContractAsync<SqlServerDeadLetterStoreContract>();

    private static async Task RunContractAsync<T>()
        where T : new()
    {
        await EnsureTablesAsync();
        var failures = new List<string>();
        var methods = typeof(T).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(method => method.GetCustomAttribute<FactAttribute>() is not null);

        foreach (var method in methods)
        {
            try
            {
                await (Task)method.Invoke(new T(), null)!;
            }
            catch (TargetInvocationException ex)
            {
                failures.Add($"{method.Name}: {ex.InnerException?.Message}");
            }
        }

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    private static async Task EnsureTablesAsync()
    {
        await using var connection = await SqlServerConnection.OpenAsync(CancellationToken.None);
        foreach (var sql in new[] { SqlServerSchema.DeadLetterTable(DeadLetterTable), SqlServerSchema.CircuitBreakerTable(BreakerTable) })
        {
            await using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }
    }

    public sealed class SqlServerCircuitBreakerStoreContract : CircuitBreakerStoreContract
    {
        protected override ICircuitBreakerStore CreateStore() =>
            new SqlServerCircuitBreakerStore(SqlServerConnection.OpenAsync, BreakerTable);
    }

    public sealed class SqlServerDeadLetterStoreContract : DeadLetterStoreContract
    {
        protected override IDeadLetterStore CreateStore(TimeProvider time) =>
            new SqlServerDeadLetterStore(SqlServerConnection.OpenAsync, DeadLetterTable, time);
    }
}

public class SqlServerTableTests
{
    [Fact]
    public void Quotes_a_valid_identifier()
    {
        Assert.Equal("[dbo].[Dead_Letter1]", new SqlServerTable("Dead_Letter1").Quoted);
    }

    [Fact]
    public void Rejects_identifiers_that_could_inject_sql()
    {
        Assert.Throws<ArgumentException>(() => new SqlServerTable("x]; DROP TABLE y;--"));
        Assert.Throws<ArgumentException>(() => new SqlServerTable("ok", "dbo.other"));
        Assert.Throws<ArgumentException>(() => new SqlServerTable(string.Empty));
        Assert.Throws<ArgumentException>(() => new SqlServerTable("1abc"));
    }

    [Fact]
    public void Ddl_uses_the_validated_name()
    {
        Assert.Contains("[dbo].[Dl]", SqlServerSchema.DeadLetterTable(new SqlServerTable("Dl")));
        Assert.Contains("[sec].[Cb]", SqlServerSchema.CircuitBreakerTable(new SqlServerTable("Cb", "sec")));
    }
}
