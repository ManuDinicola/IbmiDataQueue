using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace IbmiDataQueue.FileSystem;

/// <summary>
/// État du disjoncteur dans un fichier JSON par nom, partageable entre plusieurs processus d'une même machine
/// (ou d'un même disque partagé qui respecte les verrous de fichier : ce n'est PAS le cas de tous les partages réseau).
/// Le compare-and-swap est protégé par un fichier verrou ouvert en exclusif ; l'écriture est atomique (temporaire puis remplacement).
/// </summary>
public sealed class FileCircuitBreakerStore : ICircuitBreakerStore
{
    private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(5);

    private readonly string _directory;

    public FileCircuitBreakerStore(string directory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directory);
        _directory = Path.GetFullPath(directory);
        Directory.CreateDirectory(_directory);
    }

    public async ValueTask<CircuitSnapshot> LoadAsync(string name, CancellationToken cancellationToken)
    {
        // Lecture sans verrou : le fichier n'est jamais modifié en place, seulement remplacé en entier.
        return await ReadAsync(DataPath(name), cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask<bool> TrySaveAsync(string name, CircuitSnapshot next, long expectedVersion, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(next);
        var data = DataPath(name);

        await using var gate = await AcquireLockAsync(data + ".lock", cancellationToken).ConfigureAwait(false);

        var current = await ReadAsync(data, cancellationToken).ConfigureAwait(false);
        if (current.Version != expectedVersion)
        {
            return false;
        }

        var saved = next with { Version = expectedVersion + 1 };
        var temp = data + "." + Guid.NewGuid().ToString("N") + ".tmp";
        await using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.WriteThrough))
        {
            await JsonSerializer.SerializeAsync(stream, saved, cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }

        File.Move(temp, data, overwrite: true);
        return true;
    }

    private static async ValueTask<CircuitSnapshot> ReadAsync(string path, CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            return await JsonSerializer.DeserializeAsync<CircuitSnapshot>(stream, cancellationToken: cancellationToken).ConfigureAwait(false)
                ?? CircuitSnapshot.Initial;
        }
        catch (FileNotFoundException)
        {
            return CircuitSnapshot.Initial;
        }
    }

    private static async ValueTask<FileStream> AcquireLockAsync(string lockPath, CancellationToken cancellationToken)
    {
        var started = Environment.TickCount64;
        while (true)
        {
            try
            {
                return new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.None);
            }
            catch (IOException) when (Environment.TickCount64 - started < LockTimeout.TotalMilliseconds)
            {
                await Task.Delay(Random.Shared.Next(2, 15), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Nom de fichier sûr : le nom du disjoncteur n'est jamais utilisé comme chemin, seulement son empreinte.</summary>
    private string DataPath(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var hash = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(name)))[..32];
        return Path.Combine(_directory, "circuit-" + hash + ".json");
    }
}
