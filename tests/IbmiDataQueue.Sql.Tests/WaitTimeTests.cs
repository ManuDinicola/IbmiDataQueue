using IbmiDataQueue.Sql;
using Xunit;

namespace IbmiDataQueue.Sql.Tests;

public class WaitTimeTests
{
    [Fact]
    public void Zero_means_no_wait()
    {
        Assert.Equal(0m, SqlDataQueueClient.ToWaitSeconds(TimeSpan.Zero));
    }

    [Fact]
    public void Negative_is_clamped_to_zero()
    {
        Assert.Equal(0m, SqlDataQueueClient.ToWaitSeconds(TimeSpan.FromSeconds(-5)));
    }

    [Fact]
    public void Keeps_milliseconds()
    {
        Assert.Equal(1.5m, SqlDataQueueClient.ToWaitSeconds(TimeSpan.FromMilliseconds(1500)));
    }

    [Fact]
    public void Is_capped_at_the_maximum_accepted_by_ibm_i()
    {
        Assert.Equal(99999m, SqlDataQueueClient.ToWaitSeconds(TimeSpan.FromSeconds(200000)));
    }

    [Fact]
    public void Infinite_uses_the_maximum()
    {
        Assert.Equal(99999m, SqlDataQueueClient.ToWaitSeconds(Timeout.InfiniteTimeSpan));
    }
}
