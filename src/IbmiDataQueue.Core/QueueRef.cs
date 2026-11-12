namespace IbmiDataQueue;

/// <summary>Identifie une data queue IBM i (bibliothèque + nom).</summary>
public readonly record struct QueueRef(string Library, string Name)
{
    public override string ToString() => $"{Library}/{Name}";
}
