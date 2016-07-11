namespace NEventStore.Persistence.DocumentDB
{
    using System;
    using System.Collections.Generic;

    using Newtonsoft.Json;

    internal class DocumentCommit
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        public string BucketId { get; set; }

        public string StreamId { get; set; }

        public int CommitSequence { get; set; }

        public int StartingStreamRevision { get; set; }

        public int StreamRevision { get; set; }

        public Guid CommitId { get; set; }

        public DateEpoch CommitStamp { get; set; }

        public IDictionary<string, object> Headers { get; set; }

        [JsonProperty(TypeNameHandling = TypeNameHandling.All)]
        public IList<string> Payload { get; set; }

        public bool Dispatched { get; set; }

        public long CheckpointNumber { get; set; }
    }

    internal class DateEpoch
    {
        public DateTime Date { get; set; }
        public long Epoch
        {
            get
            {
                return (this.Date.Equals(null) || this.Date.Equals(DateTime.MinValue))
                    ? int.MinValue
                    : this.Date.ToEpoch();
            }
        }
    }
}
