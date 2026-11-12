using System.Data.Common;
using System.Text.RegularExpressions;

namespace IbmiDataQueue.SqlServer;

/// <summary>Nom de table validé puis entouré de crochets : jamais de texte libre concaténé dans le SQL.</summary>
public sealed partial class SqlServerTable
{
    public SqlServerTable(string table, string schema = "dbo")
    {
        Schema = Validate(schema, nameof(schema));
        Table = Validate(table, nameof(table));
        Quoted = $"[{Schema}].[{Table}]";
    }

    public string Schema { get; }

    public string Table { get; }

    public string Quoted { get; }

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]{0,127}$")]
    private static partial Regex IdentifierPattern();

    internal static DbCommand Command(DbConnection connection, string sql, params (string Name, object? Value)[] parameters)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var (name, value) in parameters)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Add(parameter);
        }

        return command;
    }

    private static string Validate(string identifier, string parameter) =>
        identifier is not null && IdentifierPattern().IsMatch(identifier)
            ? identifier
            : throw new ArgumentException("Identifiant SQL invalide : lettres, chiffres et _ uniquement (128 max).", parameter);
}
