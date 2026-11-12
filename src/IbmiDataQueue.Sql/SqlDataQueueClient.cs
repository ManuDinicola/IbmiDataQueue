using System.Data.Common;

namespace IbmiDataQueue.Sql;

/// <summary>
/// Lit une data queue avec la table function QSYS2.RECEIVE_DATA_QUEUE (IBM i 7.3+).
/// Le message est retiré de la queue à la lecture et lu en binaire (MESSAGE_DATA_BINARY).
/// Une connexion est ouverte par lecture et reste occupée pendant l'attente : dimensionner le pool en conséquence.
/// </summary>
public sealed class SqlDataQueueClient : IDataQueueClient
{
    /// <summary>Valeur maximale acceptée par WAIT_TIME, en secondes.</summary>
    public const decimal MaxWaitSeconds = 99999m;

    private const string ReceiveSql = """
        SELECT MESSAGE_DATA_BINARY, KEY_DATA
        FROM TABLE(
            QSYS2.RECEIVE_DATA_QUEUE(
                DATA_QUEUE => ?,
                DATA_QUEUE_LIBRARY => ?,
                REMOVE => 'YES',
                WAIT_TIME => ?
            )
        )
        """;

    private readonly Func<CancellationToken, ValueTask<DbConnection>> _openConnection;

    /// <param name="openConnection">Retourne une connexion ouverte, libérée par le client après chaque lecture.</param>
    public SqlDataQueueClient(Func<CancellationToken, ValueTask<DbConnection>> openConnection)
    {
        _openConnection = openConnection ?? throw new ArgumentNullException(nameof(openConnection));
    }

    public async ValueTask<DataQueueMessage?> ReceiveAsync(QueueRef queue, TimeSpan wait, CancellationToken cancellationToken)
    {
        await using var connection = await _openConnection(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = ReceiveSql;
        AddParameter(command, queue.Name);
        AddParameter(command, queue.Library);
        AddParameter(command, ToWaitSeconds(wait));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null; // aucune ligne : queue restée vide
        }

        var payload = await reader.IsDBNullAsync(0, cancellationToken).ConfigureAwait(false)
            ? Array.Empty<byte>()
            : reader.GetFieldValue<byte[]>(0);
        var key = await reader.IsDBNullAsync(1, cancellationToken).ConfigureAwait(false)
            ? null
            : reader.GetString(1);

        return new DataQueueMessage(payload, key);
    }

    /// <summary>Convertit une attente en secondes pour WAIT_TIME : bornée à [0, 99999], millisecondes conservées.</summary>
    public static decimal ToWaitSeconds(TimeSpan wait)
    {
        if (wait == Timeout.InfiniteTimeSpan)
        {
            return MaxWaitSeconds;
        }

        var seconds = Math.Round((decimal)wait.TotalSeconds, 3);
        return Math.Clamp(seconds, 0m, MaxWaitSeconds);
    }

    private static void AddParameter(DbCommand command, object value)
    {
        var parameter = command.CreateParameter();
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }
}
