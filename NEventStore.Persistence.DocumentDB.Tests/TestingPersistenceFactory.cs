namespace NEventStore.Persistence.DocumentDB.Tests
{
    using System.Configuration;

    public class TestingPersistenceFactory : DocumentPersistenceFactory
    {
        public TestingPersistenceFactory()
            : base(TestingFactoryValues.Uri, TestingFactoryValues.PrimaryKey, TestingFactoryValues.Options)
        {
        }

        private static class TestingFactoryValues
        {
            public static string Uri
            {
                get { return ConfigurationManager.AppSettings["NEventStore/Persistence/EndpointUri"]; }
            }

            public static string PrimaryKey
            {
                get { return ConfigurationManager.AppSettings["NEventStore/Persistence/PrimaryKey"]; }
            }

            public static DocumentPersistenceOptions Options
            {
                get
                {
                    return new DocumentPersistenceOptions("AcceptanceTestsEventStore", "Commits", "Streams");
                }
            }
        }
    }


}
