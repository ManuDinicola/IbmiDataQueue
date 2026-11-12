using IbmiDataQueue.FileSystem;

namespace IbmiDataQueue.Core.Tests;

public sealed class FileSystemCircuitBreakerStoreTests : CircuitBreakerStoreContract, IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "dq-cb-" + Guid.NewGuid().ToString("N"));

    protected override ICircuitBreakerStore CreateStore() => new FileCircuitBreakerStore(_directory);

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}

public sealed class FileSystemDeadLetterStoreTests : DeadLetterStoreContract, IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), "dq-dl-" + Guid.NewGuid().ToString("N"));

    protected override IDeadLetterStore CreateStore(TimeProvider time) => new FileSystemDeadLetterStore(_directory, time);

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }
}
