using System.Data.Common;
using System.Data.Odbc;

namespace IbmiDataQueue.IntegrationTests;

/// <summary>Seul point dépendant du driver : remplacer ce fichier pour utiliser IBM.Data.Db2.</summary>
internal static class IntegrationConnection
{
    public static async ValueTask<DbConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new OdbcConnection(IntegrationSettings.ConnectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }
}
