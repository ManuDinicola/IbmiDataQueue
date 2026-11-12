namespace IbmiDataQueue.Core.Tests;

public class InMemoryCircuitBreakerStoreTests : CircuitBreakerStoreContract
{
    protected override ICircuitBreakerStore CreateStore() => new InMemoryCircuitBreakerStore();
}
