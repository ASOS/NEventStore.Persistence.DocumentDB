 // ReSharper disable once CheckNamespace
namespace NEventStore
{
    using NEventStore.Persistence.DocumentDB;

    public static class DocumentPersistanceWireupExtension
    {
        public static DocumentPersistenceWireup UsingDocumentPersistence(
            this Wireup wireup,
            string url,
            string key)
        {
            return new DocumentPersistenceWireup(wireup, url, key);
        }

        public static DocumentPersistenceWireup UsingDocumentPersistence(
            this Wireup wireup,
            string url,
            string key,
            DocumentPersistenceOptions options)
        {
            return new DocumentPersistenceWireup(wireup, url, key, options);
        }
    }
}
