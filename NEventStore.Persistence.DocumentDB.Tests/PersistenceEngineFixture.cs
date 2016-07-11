// ReSharper disable once CheckNamespace
namespace NEventStore.Persistence.AcceptanceTests
{
    using NEventStore.Persistence.DocumentDB.Tests;

    public partial class PersistenceEngineFixture
    {
        public PersistenceEngineFixture()
        {
            this.createPersistence = () => new TestingPersistenceFactory().Build();
        }
    }
}