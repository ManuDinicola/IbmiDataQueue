using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace IbmiDataQueue.SqlServer.Tests;

/// <summary>Seul fichier qui dépend du pilote SQL Server.</summary>
internal static class SqlServerConnection
{
    public const string Variable = "SQLSERVER_CONNECTION_STRING";

    public static string? ConnectionString => Environment.GetEnvironmentVariable(Variable);

    public static async ValueTask<DbConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(ConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }
}
