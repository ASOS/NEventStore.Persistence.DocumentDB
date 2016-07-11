namespace NEventStore.Persistence.DocumentDB
{
    public class DocumentPersistenceFactory : IPersistenceFactory
    {
        public DocumentPersistenceFactory(string url, string key, DocumentPersistenceOptions options)
        {
            this.Url = url;
            this.Key = key;
            this.Options = options;
        }

        public string Url { get; private set; }

        public string Key { get; private set; }

        public DocumentPersistenceOptions Options { get; private set; }

        public IPersistStreams Build()
        {
            return new DocumentPersistenceEngine(this.Options.GetDocumentClient(this.Url, this.Key), this.Options);
        }
    }
}
