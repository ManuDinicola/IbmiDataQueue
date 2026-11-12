namespace IbmiDataQueue.Testing;

/// <summary>Handler défini par une lambda, pour écrire des tests sans classe dédiée.</summary>
public sealed class DelegateMessageHandler<T> : IMessageHandler<T>
{
    private readonly Func<T, CancellationToken, ValueTask> _handle;

    public DelegateMessageHandler(Func<T, CancellationToken, ValueTask> handle)
    {
        _handle = handle ?? throw new ArgumentNullException(nameof(handle));
    }

    public ValueTask HandleAsync(T message, CancellationToken cancellationToken) => _handle(message, cancellationToken);
}
