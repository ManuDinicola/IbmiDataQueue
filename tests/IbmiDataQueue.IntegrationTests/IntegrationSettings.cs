namespace IbmiDataQueue.IntegrationTests;

/// <summary>
/// Configuration par variables d'environnement (aucun identifiant dans le code) :
/// IBMI_HOST, IBMI_USER, IBMI_PASSWORD, IBMI_LIBRARY (obligatoires, la bibliothèque doit exister),
/// IBMI_CONNECTION_STRING (remplace HOST/USER/PASSWORD), IBMI_ODBC_DRIVER, IBMI_CODEPAGE (EBCDIC, 37 par défaut).
/// </summary>
internal static class IntegrationSettings
{
    private const string DefaultDriver = "IBM i Access ODBC Driver";

    public static string Library => Get("IBMI_LIBRARY") ?? string.Empty;

    public static int CodePage => int.TryParse(Get("IBMI_CODEPAGE"), out var codePage) ? codePage : 37;

    public static string? ConnectionString
    {
        get
        {
            var explicitString = Get("IBMI_CONNECTION_STRING");
            if (explicitString is not null)
            {
                return explicitString;
            }

            var host = Get("IBMI_HOST");
            var user = Get("IBMI_USER");
            var password = Get("IBMI_PASSWORD");
            if (host is null || user is null || password is null)
            {
                return null;
            }

            var driver = Get("IBMI_ODBC_DRIVER") ?? DefaultDriver;
            var escapedPassword = password.Replace("}", "}}", StringComparison.Ordinal);
            return $"Driver={{{driver}}};System={host};Uid={user};Pwd={{{escapedPassword}}};";
        }
    }

    public static bool IsConfigured => Library.Length > 0 && ConnectionString is not null;

    private static string? Get(string name)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
