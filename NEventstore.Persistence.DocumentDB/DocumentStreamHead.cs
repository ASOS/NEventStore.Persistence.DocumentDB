namespace NEventStore.Persistence.DocumentDB
{
    using Newtonsoft.Json;

    class DocumentStreamHead
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
        public string BucketId { get; set; }
        public string StreamId { get; set; }
        public int HeadRevision { get; set; }
        public int SnapshotRevision { get; set; }

        public int SnapshotAge
        {
            get { return this.HeadRevision - this.SnapshotRevision; }
        }

        public static string GetStreamHeadId(string bucketId, string streamId)
        {
            return string.Format("StreamHeads-{0}-{1}", bucketId, streamId);
        }
    }
}
