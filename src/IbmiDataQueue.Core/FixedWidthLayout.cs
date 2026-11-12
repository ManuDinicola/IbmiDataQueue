using System.Text;

namespace IbmiDataQueue;

/// <summary>Le message ne respecte pas le format attendu.</summary>
public sealed class MessageDecodeException : Exception
{
    public MessageDecodeException(string message) : base(message) { }

    public MessageDecodeException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Découpe un message à champs de largeur fixe. Les largeurs sont en octets :
/// utiliser un encodage mono-octet (ASCII, IBM037, IBM500, IBM1148...), celui du job émetteur.
/// </summary>
public sealed class FixedWidthLayout
{
    private readonly int[] _lengths;
    private readonly Encoding _encoding;
    private readonly bool _allowLonger;

    public FixedWidthLayout(Encoding encoding, IReadOnlyList<int> fieldLengths, bool allowLonger = false)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        ArgumentNullException.ThrowIfNull(fieldLengths);
        if (fieldLengths.Count == 0 || fieldLengths.Any(l => l <= 0))
        {
            throw new ArgumentException("Chaque champ doit avoir une largeur strictement positive.", nameof(fieldLengths));
        }

        _encoding = encoding;
        _lengths = fieldLengths.ToArray();
        _allowLonger = allowLonger;
        TotalLength = _lengths.Sum();
    }

    public int TotalLength { get; }

    public int FieldCount => _lengths.Length;

    /// <summary>Retourne les champs décodés et débarrassés de leur remplissage (espaces).</summary>
    /// <exception cref="MessageDecodeException">Longueur incorrecte (stricte par défaut : trop long est aussi refusé).</exception>
    public string[] Read(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < TotalLength || (!_allowLonger && payload.Length > TotalLength))
        {
            var expected = _allowLonger ? $"au moins {TotalLength}" : $"exactement {TotalLength}";
            throw new MessageDecodeException(
                $"Longueur de message invalide : {expected} octets attendus, {payload.Length} reçus.");
        }

        var fields = new string[_lengths.Length];
        var offset = 0;
        for (var index = 0; index < _lengths.Length; index++)
        {
            fields[index] = _encoding.GetString(payload.Slice(offset, _lengths[index])).Trim();
            offset += _lengths[index];
        }

        return fields;
    }
}
