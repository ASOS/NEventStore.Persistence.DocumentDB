namespace NEventStore.Persistence.DocumentDB
{
    using System;

    using Microsoft.Azure.Documents.Client;

    public class DocumentPersistenceOptions
    {
        private const string DefaultDatabaseName = "NEventStore";
        private const string DefaultCommitCollectionName = "Commits";
        private const string DefaultStreamHeadCollectionName = "Streams";
        private const string DefaultSnapshotCollectionName = "Snapshots";

        public DocumentPersistenceOptions(
            string databaseName = DefaultDatabaseName,
            string commitCollectionName = DefaultCommitCollectionName,
            string streamHeadCollectionName = DefaultStreamHeadCollectionName,
            string snapshotCollectionName = DefaultSnapshotCollectionName)
        {
            this.DatabaseName = databaseName;
            this.CommitCollectionName = commitCollectionName;
            this.StreamHeadCollectionName = streamHeadCollectionName;
            this.SnapshotCollectionName = snapshotCollectionName;
        }

        public string DatabaseName { get; private set; }
        public string CommitCollectionName { get; set; }
        public string StreamHeadCollectionName { get; set; }
        public string SnapshotCollectionName { get; set; }

        internal DocumentClient GetDocumentClient(string url, string key)
        {
            return new DocumentClient(new Uri(url), key);
        }

    }
}
