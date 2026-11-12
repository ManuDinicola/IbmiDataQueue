using System.Data.Common;
using System.Data.Odbc;
using System.Globalization;
using System.Text;
using IbmiDataQueue;
using IbmiDataQueue.Demo.Web;
using IbmiDataQueue.Hosting;
using IbmiDataQueue.Sql;

// Configuration par variables d'environnement (aucun identifiant dans le code) :
// IBMI_HOST, IBMI_USER, IBMI_PASSWORD, IBMI_LIBRARY (obligatoires), IBMI_QUEUE (DEMOQ par défaut),
// IBMI_ODBC_DRIVER ("IBM i Access ODBC Driver" par défaut).
static string Required(string name) =>
    Environment.GetEnvironmentVariable(name) is { Length: > 0 } value
        ? value
        : throw new InvalidOperationException($"Variable d'environnement {name} manquante.");

static string Optional(string name, string fallback) =>
    Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;

var host = Required("IBMI_HOST");
var user = Required("IBMI_USER");
var password = Required("IBMI_PASSWORD").Replace("}", "}}", StringComparison.Ordinal);
var queue = new QueueRef(Required("IBMI_LIBRARY"), Optional("IBMI_QUEUE", "DEMOQ"));
var driver = Optional("IBMI_ODBC_DRIVER", "IBM i Access ODBC Driver");
var connectionString = $"Driver={{{driver}}};System={host};Uid={user};Pwd={{{password}}};";

// Seul point dépendant du pilote.
async ValueTask<DbConnection> OpenAsync(CancellationToken cancellationToken)
{
    var connection = new OdbcConnection(connectionString);
    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
    return connection;
}

async Task<object?> RunAsync(string sql, params object[] values)
{
    await using var connection = await OpenAsync(CancellationToken.None);
    await using var command = connection.CreateCommand();
    command.CommandText = sql;
    foreach (var value in values)
    {
        var parameter = command.CreateParameter();
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    return await command.ExecuteScalarAsync();
}

// Crée la data queue (30 octets = Id 6 + Type 4 + Payload 20) si elle n'existe pas encore.
var existing = Convert.ToInt32(
    await RunAsync(
        "SELECT COUNT(*) FROM QSYS2.DATA_QUEUE_INFO WHERE DATA_QUEUE_LIBRARY = ? AND DATA_QUEUE_NAME = ?",
        queue.Library,
        queue.Name),
    CultureInfo.InvariantCulture);
if (existing == 0)
{
    await RunAsync($"CALL QSYS2.QCMDEXC('CRTDTAQ DTAQ({queue.Library}/{queue.Name}) MAXLEN(30)')");
    Console.WriteLine($"Data queue {queue} créée.");
}

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<Feed>();
builder.Services.AddSingleton<IDataQueueClient>(_ => new SqlDataQueueClient(OpenAsync));
builder.Services.AddSingleton<IDeadLetterSink, FeedDeadLetterSink>();
builder.Services.AddDataQueueConsumer<DemoMessage, DemoCodec, FeedHandler>(
    queue,
    options => options.Wait = TimeSpan.FromSeconds(3));

var app = builder.Build();
app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/api/state", (Feed feed) => new { queue = queue.ToString(), received = feed.Received, deadLetters = feed.DeadLetters });

// Publie un message : soit les 3 champs (formatés sur 30 octets), soit un texte brut libre (pour provoquer un rejet).
app.MapPost("/api/publish", async (PublishRequest request) =>
{
    byte[] payload;
    if (!string.IsNullOrEmpty(request.Raw))
    {
        payload = Encoding.ASCII.GetBytes(request.Raw);
        if (payload.Length > 30)
        {
            return Results.BadRequest(new { error = "La data queue accepte 30 octets maximum (MAXLEN 30)." });
        }
    }
    else
    {
        var id = request.Id ?? string.Empty;
        var type = request.Type ?? string.Empty;
        var text = request.Payload ?? string.Empty;
        if (id.Length > 6 || type.Length > 4 || text.Length > 20)
        {
            return Results.BadRequest(new { error = "Id max 6, Type max 4, Payload max 20 caractères." });
        }

        payload = DemoCodec.Encode(id, type, text);
    }

    try
    {
        await RunAsync(
            "CALL QSYS2.SEND_DATA_QUEUE_BINARY(MESSAGE_DATA => ?, DATA_QUEUE => ?, DATA_QUEUE_LIBRARY => ?)",
            payload,
            queue.Name,
            queue.Library);
    }
    catch (DbException ex)
    {
        return Results.Json(new { error = ex.Message }, statusCode: 502);
    }

    return Results.Ok(new { sent = payload.Length });
});

app.Run();

internal sealed record PublishRequest(string? Id, string? Type, string? Payload, string? Raw);
