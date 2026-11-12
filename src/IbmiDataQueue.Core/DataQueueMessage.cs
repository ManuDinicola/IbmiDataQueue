namespace IbmiDataQueue;

/// <summary>Message brut reçu d'une data queue : des octets, jamais du texte interprété.</summary>
/// <param name="Payload">Contenu binaire du message.</param>
/// <param name="Key">Clé du message pour une data queue à clé, sinon null.</param>
public sealed record DataQueueMessage(ReadOnlyMemory<byte> Payload, string? Key = null);
