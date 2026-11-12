using Xunit;

namespace IbmiDataQueue.SqlServer.Tests;

/// <summary>Test ignoré tant que SQLSERVER_CONNECTION_STRING n'est pas défini.</summary>
public sealed class SqlServerFactAttribute : FactAttribute
{
    public SqlServerFactAttribute()
    {
        if (string.IsNullOrWhiteSpace(SqlServerConnection.ConnectionString))
        {
            Skip = $"Définir {SqlServerConnection.Variable} (base de test jetable) pour lancer les tests SQL Server.";
        }
    }
}
