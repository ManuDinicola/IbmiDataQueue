using System.Text;

namespace IbmiDataQueue.Core.Tests;

/// <summary>Domaine neutre pour les tests : aucun lien avec un métier réel.</summary>
public sealed record Echo(string Id, string Type, string Payload);

/// <summary>Layout fixe : Id(6) + Type(4) + Payload(20) = 30 octets.</summary>
public sealed class EchoCodec : IMessageCodec<Echo>
{
    public static readonly FixedWidthLayout Layout = new(Encoding.ASCII, [6, 4, 20]);

    public Echo Decode(ReadOnlySpan<byte> payload)
    {
        var fields = Layout.Read(payload);
        return new Echo(fields[0], fields[1], fields[2]);
    }
}

public static class EchoMessages
{
    public static byte[] Make(string id, string type, string payload) =>
        Encoding.ASCII.GetBytes(id.PadRight(6) + type.PadRight(4) + payload.PadRight(20));
}
