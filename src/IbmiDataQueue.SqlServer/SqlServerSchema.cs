namespace IbmiDataQueue.SqlServer;

/// <summary>Scripts de création des tables. À exécuter par vos migrations (la bibliothèque ne modifie jamais le schéma toute seule).</summary>
public static class SqlServerSchema
{
    public static string DeadLetterTable(SqlServerTable table) => $"""
        IF OBJECT_ID(N'{table.Schema}.{table.Table}', N'U') IS NULL
        CREATE TABLE {table.Quoted} (
            Id            char(32)       NOT NULL CONSTRAINT [PK_{table.Table}] PRIMARY KEY,
            ReceivedAtUtc datetime2(3)   NOT NULL,
            Library       nvarchar(10)   NOT NULL,
            QueueName     nvarchar(10)   NOT NULL,
            MessageKey    nvarchar(256)  NULL,
            Reason        nvarchar(32)   NOT NULL,
            Attempts      int            NOT NULL,
            ErrorType     nvarchar(512)  NOT NULL,
            ErrorMessage  nvarchar(max)  NOT NULL,
            ErrorDetail   nvarchar(max)  NOT NULL,
            Payload       varbinary(max) NOT NULL
        );
        """;

    public static string CircuitBreakerTable(SqlServerTable table) => $"""
        IF OBJECT_ID(N'{table.Schema}.{table.Table}', N'U') IS NULL
        CREATE TABLE {table.Quoted} (
            Name                nvarchar(200) NOT NULL CONSTRAINT [PK_{table.Table}] PRIMARY KEY,
            State               tinyint       NOT NULL,
            ConsecutiveFailures int           NOT NULL,
            OpenUntilTicks      bigint        NOT NULL,
            ProbeUntilTicks     bigint        NOT NULL,
            Version             bigint        NOT NULL
        );
        """;
}
