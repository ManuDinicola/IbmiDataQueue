using System.Diagnostics;
using System.Text;
using IbmiDataQueue.Core.Tests;
using IbmiDataQueue.Sql;
using IbmiDataQueue.Testing;
using Xunit;

namespace IbmiDataQueue.IntegrationTests;

/// <summary>
/// Tests contre un vrai IBM i. Chaque test crée sa propre data queue (30 octets, FIFO) dans IBMI_LIBRARY
/// avec QSYS2.QCMDEXC, puis la supprime. Aucun programme COBOL, physical file ni terminal requis.
/// Lancer : dotnet test --filter Category=Integration
/// </summary>
[Trait("Category", "Integration")]
public class DataQueueIntegrationTests : IAsyncLifetime
{
    private readonly string _library = IntegrationSettings.Library;
    private readonly string _name = "DQT" + Guid.NewGuid().ToString("N")[..6].ToUpperInvariant();
    private readonly SqlDataQueueClient _client = new(IntegrationConnection.OpenAsync);

    private QueueRef Queue => new(_library, _name);

    public async Task InitializeAsync() =>
        await ExecuteAsync($"CALL QSYS2.QCMDEXC('CRTDTAQ DTAQ({_library}/{_name}) MAXLEN(30)')");

    public async Task DisposeAsync()
    {
        // IBMI_KEEP_QUEUE=1 : conserver les queues de test pour les inspecter sur l'IBM i.
        if (Environment.GetEnvironmentVariable("IBMI_KEEP_QUEUE") == "1")
        {
            return;
        }

        try
        {
            await ExecuteAsync($"CALL QSYS2.QCMDEXC('DLTDTAQ DTAQ({_library}/{_name})')");
        }
        catch (Exception)
        {
            // Nettoyage au mieux : ne pas masquer le résultat du test.
        }
    }

    [IntegrationFact]
    public async Task Binary_message_round_trips_unchanged()
    {
        var payload = EchoMessages.Make("42", "NEW", "hello");
        await SendBinaryAsync(payload);

        var message = await _client.ReceiveAsync(Queue, TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.NotNull(message);
        Assert.Equal(payload, message!.Payload.ToArray());
        Assert.Null(message.Key);
    }

    [IntegrationFact]
    public async Task Empty_queue_returns_no_message_after_the_wait()
    {
        var stopwatch = Stopwatch.StartNew();
        var message = await _client.ReceiveAsync(Queue, TimeSpan.FromSeconds(2), CancellationToken.None);
        stopwatch.Stop();

        // Hypothèse du client SQL : une queue vide = zéro ligne. Si ce test échoue, voir SqlDataQueueClient.ReceiveAsync.
        Assert.Null(message);
        Assert.True(
            stopwatch.Elapsed >= TimeSpan.FromSeconds(1),
            $"L'attente n'a pas été respectée ({stopwatch.Elapsed.TotalSeconds:F1} s).");
    }

    [IntegrationFact]
    public async Task Pipeline_handles_a_valid_message_and_dead_letters_an_invalid_one()
    {
        var handled = new List<Echo>();
        var deadLetters = new InMemoryDeadLetterSink();
        var pipeline = new DataQueuePipeline<Echo>(
            _client,
            Queue,
            new EchoCodec(),
            new DelegateMessageHandler<Echo>((message, _) =>
            {
                handled.Add(message);
                return ValueTask.CompletedTask;
            }),
            deadLetters,
            new PipelineOptions { Wait = TimeSpan.FromSeconds(3), MaxHandlerAttempts = 1 });
        await SendBinaryAsync(EchoMessages.Make("1", "NEW", "ok"));
        await SendBinaryAsync(new byte[] { 1, 2, 3 });

        var first = await pipeline.ProcessNextAsync(CancellationToken.None);
        var second = await pipeline.ProcessNextAsync(CancellationToken.None);
        var third = await pipeline.ProcessNextAsync(CancellationToken.None);

        Assert.Equal(ProcessResult.Handled, first);
        Assert.Equal(ProcessResult.DeadLettered, second);
        Assert.Equal(ProcessResult.NoMessage, third);
        Assert.Equal(new Echo("1", "NEW", "ok"), Assert.Single(handled));
        Assert.Equal(DeadLetterReason.DecodeFailed, Assert.Single(deadLetters.Items).Reason);
    }

    /// <summary>
    /// Message texte (sans espaces, donc indépendant du traitement des blancs) converti par IBM i dans le CCSID du job.
    /// Si ce test échoue, ajuster IBMI_CODEPAGE (37, 500, 1148...) selon le CCSID du job.
    /// </summary>
    [IntegrationFact]
    public async Task Text_message_is_decoded_with_the_job_ebcdic_code_page()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var layout = new FixedWidthLayout(Encoding.GetEncoding(IntegrationSettings.CodePage), [6, 4, 20]);
        await SendTextAsync("A00042NEW1HELLOWORLD0123456789");

        var message = await _client.ReceiveAsync(Queue, TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.NotNull(message);
        Assert.Equal(new[] { "A00042", "NEW1", "HELLOWORLD0123456789" }, layout.Read(message!.Payload.Span));
    }

    private Task SendBinaryAsync(byte[] payload) =>
        ExecuteAsync(
            "CALL QSYS2.SEND_DATA_QUEUE_BINARY(MESSAGE_DATA => ?, DATA_QUEUE => ?, DATA_QUEUE_LIBRARY => ?)",
            payload,
            _name,
            _library);

    private Task SendTextAsync(string text) =>
        ExecuteAsync(
            "CALL QSYS2.SEND_DATA_QUEUE(MESSAGE_DATA => ?, DATA_QUEUE => ?, DATA_QUEUE_LIBRARY => ?)",
            text,
            _name,
            _library);

    private static async Task ExecuteAsync(string sql, params object[] values)
    {
        await using var connection = await IntegrationConnection.OpenAsync(CancellationToken.None);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var value in values)
        {
            var parameter = command.CreateParameter();
            parameter.Value = value;
            command.Parameters.Add(parameter);
        }

        await command.ExecuteNonQueryAsync();
    }
}
