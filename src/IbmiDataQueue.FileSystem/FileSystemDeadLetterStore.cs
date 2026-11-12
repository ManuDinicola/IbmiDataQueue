using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace IbmiDataQueue.FileSystem;

/// <summary>
/// Dead-letter sur disque : un fichier JSON par message (payload en base64), écrit de façon atomique (fichier temporaire puis renommage).
/// Les noms de fichier sont des identifiants hexadécimaux générés ici : aucun nom venant d'ailleurs n'est utilisé comme chemin.
/// </summary>
public sealed partial class FileSystemDeadLetterStore : IDeadLetterStore
{
    private const string Extension = ".json";

    private readonly string _directory;
    private readonly TimeProvider _time;

    public FileSystemDeadLetterStore(string directory, TimeProvider? timeProvider = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        _directory = Path.GetFullPath(directory);
        _time = timeProvider ?? TimeProvider.System;
        Directory.CreateDirectory(_directory);
    }

    public async ValueTask WriteAsync(DeadLetter letter, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(letter);

        var now = _time.GetUtcNow();
        var id = Guid.CreateVersion7(now).ToString("N");
        var record = new Record
        {
            Id = id,
            ReceivedAtUtc = now,
            Library = letter.Queue.Library,
            QueueName = letter.Queue.Name,
            PayloadBase64 = Convert.ToBase64String(letter.Message.Payload.Span),
            Key = letter.Message.Key,
            Reason = letter.Reason.ToString(),
            Attempts = letter.Attempts,
            ErrorType = letter.Error?.GetType().FullName ?? string.Empty,
            ErrorMessage = letter.Error?.Message ?? string.Empty,
            ErrorDetail = letter.Error?.ToString() ?? string.Empty,
        };

        var temp = Path.Combine(_directory, id + ".tmp");
        var target = PathFor(id);
        await using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
        {
            await JsonSerializer.SerializeAsync(stream, record, cancellationToken: CancellationToken.None).ConfigureAwait(false);
            await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }

        File.Move(temp, target, overwrite: false);
    }

    public async IAsyncEnumerable<StoredDeadLetter> ListAsync(int max, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (max <= 0)
        {
            yield break;
        }

        // Les identifiants sont des UUID v7 : l'ordre des noms est l'ordre chronologique.
        var files = Directory.EnumerateFiles(_directory, "*" + Extension)
            .Where(path => IdPattern().IsMatch(Path.GetFileNameWithoutExtension(path)))
            .Order(StringComparer.Ordinal)
            .Take(max);

        foreach (var file in files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Record? record;
            try
            {
                await using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                record = await JsonSerializer.DeserializeAsync<Record>(stream, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (FileNotFoundException)
            {
                continue; // supprimé entre l'énumération et la lecture
            }

            if (record is null)
            {
                continue;
            }

            yield return new StoredDeadLetter(
                record.Id,
                record.ReceivedAtUtc,
                new QueueRef(record.Library, record.QueueName),
                Convert.FromBase64String(record.PayloadBase64),
                record.Key,
                Enum.Parse<DeadLetterReason>(record.Reason),
                record.Attempts,
                record.ErrorType,
                record.ErrorMessage,
                record.ErrorDetail);
        }
    }

    public ValueTask<bool> DeleteAsync(string id, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(id) || !IdPattern().IsMatch(id))
        {
            return ValueTask.FromResult(false);
        }

        var path = PathFor(id);
        if (!File.Exists(path))
        {
            return ValueTask.FromResult(false);
        }

        try
        {
            File.Delete(path);
            return ValueTask.FromResult(true);
        }
        catch (FileNotFoundException)
        {
            return ValueTask.FromResult(false);
        }
    }

    [GeneratedRegex("^[0-9a-f]{32}$")]
    private static partial Regex IdPattern();

    private string PathFor(string id) => Path.Combine(_directory, id + Extension);

    private sealed class Record
    {
        public string Id { get; set; } = string.Empty;

        public DateTimeOffset ReceivedAtUtc { get; set; }

        public string Library { get; set; } = string.Empty;

        public string QueueName { get; set; } = string.Empty;

        public string PayloadBase64 { get; set; } = string.Empty;

        public string? Key { get; set; }

        public string Reason { get; set; } = string.Empty;

        public int Attempts { get; set; }

        public string ErrorType { get; set; } = string.Empty;

        public string ErrorMessage { get; set; } = string.Empty;

        public string ErrorDetail { get; set; } = string.Empty;
    }
}
