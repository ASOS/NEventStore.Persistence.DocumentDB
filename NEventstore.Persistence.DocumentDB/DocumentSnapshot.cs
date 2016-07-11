namespace NEventStore.Persistence.DocumentDB
{
    using Newtonsoft.Json;

    internal class DocumentSnapshot
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        public string SnapshotId
        {
            get
            {
                return this.Id;
            }
        }
        public string BucketId { get; set; }
        public string StreamId { get; set; }
        public int StreamRevision { get; set; }
        public string Payload { get; set; }
    }
}
