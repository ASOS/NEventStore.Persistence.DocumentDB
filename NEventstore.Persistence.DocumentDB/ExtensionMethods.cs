namespace NEventStore.Persistence.DocumentDB
{
    using System;
    using System.Globalization;
    using System.Linq;

    using NEventStore.Serialization;
    using Newtonsoft.Json;

    static class ExtensionMethods
    {
        private static readonly DocumentObjectSerializer DocumentSerializer = new DocumentObjectSerializer();
        private static readonly JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.Auto
        };

        public static string ToDocumentCommitId(this CommitAttempt commit)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", commit.BucketId, commit.StreamId, commit.CommitSequence);
        }

        public static string ToDocumentCommitId(this ICommit commit)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}-{2}", commit.BucketId, commit.StreamId, commit.CommitSequence);
        }

        public static DocumentCommit ToDocumentCommit(this CommitAttempt commit, LongCheckpoint checkPoint)
        {
            var documentCommit = new DocumentCommit
            {
                Id = ToDocumentCommitId(commit),
                BucketId = commit.BucketId,
                StreamId = commit.StreamId,
                CommitSequence = commit.CommitSequence,
                StartingStreamRevision = commit.StreamRevision - (commit.Events.Count - 1),
                StreamRevision = commit.StreamRevision,
                CommitId = commit.CommitId,
                CommitStamp = new DateEpoch { Date = commit.CommitStamp },
                Headers = commit.Headers.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
                Payload = commit.Events.Select(s => JsonConvert.SerializeObject(s, Formatting.Indented, jsonSerializerSettings).ToString()).ToList(),
                CheckpointNumber = checkPoint.LongValue
            };
            return documentCommit;
        }

        public static ICommit ToCommit(this DocumentCommit commit)
        {
            var commit1 = new Commit(
                commit.BucketId,
                commit.StreamId,
                commit.StreamRevision,
                commit.CommitId,
                commit.CommitSequence,
                commit.CommitStamp.Date,
                commit.CheckpointNumber.ToString(CultureInfo.InvariantCulture),
                commit.Headers,
                commit.Payload.Select(s => JsonConvert.DeserializeObject<EventMessage>(s, jsonSerializerSettings)).ToList()
                );
            return commit1;
        }

        public static string ToDocumentSnapshotId(ISnapshot snapshot)
        {
            return string.Format("{0}-{1}-{2}", snapshot.BucketId, snapshot.StreamId, snapshot.StreamRevision);
        }

        public static DocumentSnapshot ToDocumentSnapshot(this ISnapshot snapshot)
        {
            return new DocumentSnapshot
            {
                Id = ToDocumentSnapshotId(snapshot),
                BucketId = snapshot.BucketId,
                StreamId = snapshot.StreamId,
                StreamRevision = snapshot.StreamRevision,
                Payload = DocumentSerializer.Deserialize<string>(snapshot.Payload)
            };
        }

        public static Snapshot ToSnapshot(this DocumentSnapshot snapshot)
        {
            if (snapshot == null)
            {
                return null;
            }

            return new Snapshot(snapshot.StreamId, snapshot.StreamRevision, DocumentSerializer.Serialize(snapshot.Payload));
        }

        public static DocumentStreamHead ToDocumentStreamHead(this CommitAttempt commit)
        {
            return new DocumentStreamHead
            {
                Id = DocumentStreamHead.GetStreamHeadId(commit.BucketId, commit.StreamId),
                BucketId = commit.BucketId,
                StreamId = commit.StreamId,
                HeadRevision = commit.StreamRevision,
                SnapshotRevision = 0
            };
        }

        public static DocumentStreamHead ToDocumentStreamHead(this ISnapshot snapshot)
        {
            return new DocumentStreamHead
            {
                Id = DocumentStreamHead.GetStreamHeadId(snapshot.BucketId, snapshot.StreamId),
                BucketId = snapshot.BucketId,
                StreamId = snapshot.StreamId,
                HeadRevision = snapshot.StreamRevision,
                SnapshotRevision = snapshot.StreamRevision
            };
        }

        public static StreamHead ToStreamHead(this DocumentStreamHead streamHead)
        {
            return new StreamHead(streamHead.BucketId, streamHead.StreamId, streamHead.HeadRevision, streamHead.SnapshotRevision);
        }

        public static long ToEpoch(this DateTime date)
        {
            var epoch = new DateTime(1970, 1, 1);
            var epochTimeSpan = date - epoch;
            return (long)epochTimeSpan.TotalSeconds;
        }
    }
}
