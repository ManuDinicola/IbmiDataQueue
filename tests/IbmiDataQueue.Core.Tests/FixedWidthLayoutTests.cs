using System.Text;
using Xunit;

namespace IbmiDataQueue.Core.Tests;

public class FixedWidthLayoutTests
{
    [Fact]
    public void Reads_and_trims_fields()
    {
        var fields = EchoCodec.Layout.Read(EchoMessages.Make("42", "NEW", "hello"));

        Assert.Equal(new[] { "42", "NEW", "hello" }, fields);
    }

    [Fact]
    public void Rejects_message_that_is_too_short()
    {
        var ex = Assert.Throws<MessageDecodeException>(() => EchoCodec.Layout.Read(new byte[5]));

        Assert.Contains("30", ex.Message);
    }

    [Fact]
    public void Rejects_message_that_is_too_long_by_default()
    {
        Assert.Throws<MessageDecodeException>(() => EchoCodec.Layout.Read(new byte[31]));
    }

    [Fact]
    public void Accepts_longer_message_when_allowed()
    {
        var layout = new FixedWidthLayout(Encoding.ASCII, [6, 4, 20], allowLonger: true);
        var payload = Encoding.ASCII.GetBytes("42    NEW hello               EXTRA");

        var fields = layout.Read(payload);

        Assert.Equal("hello", fields[2]);
    }

    [Fact]
    public void Decodes_ebcdic()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var ebcdic = Encoding.GetEncoding(37); // IBM037
        var layout = new FixedWidthLayout(ebcdic, [2, 2]);
        var payload = ebcdic.GetBytes("AB  ");

        Assert.Equal(0xC1, payload[0]); // 'A' en EBCDIC
        Assert.Equal(new[] { "AB", string.Empty }, layout.Read(payload));
    }
}
