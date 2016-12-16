namespace NEventStore.Persistence.DocumentDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    using NEventStore.Logging;
    using NEventStore.Persistence.DocumentDB.Constraints;
    using NEventStore.Serialization;

    public class DocumentPersistenceEngine : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(DocumentPersistenceEngine));

        private readonly Func<long> getLastCheckPointNumber;
        private readonly Func<LongCheckpoint> getNextCheckPointNumber;
        private readonly LongCheckpoint checkPointZero;
        private readonly DocumentClient client;
        private readonly DocumentPersistenceOptions options;

        public DocumentPersistenceEngine(DocumentClient client, DocumentPersistenceOptions options)
        {
            this.client = client;
            this.options = options;

            this.getLastCheckPointNumber = () => this.TryExecute(() =>
            {
                var max = this.QueryCommits()
                    .OrderByDescending(s => s.CheckpointNumber)
                    .Take(1)
                    .ToList()
                    .FirstOrDefault();

                return max != null ? max.CheckpointNumber : 0L;
            });

            this.getNextCheckPointNumber = () => new LongCheckpoint(this.getLastCheckPointNumber() + 1L);
            this.checkPointZero = new LongCheckpoint(0);
        }

        public bool IsDisposed { get; private set; }

        public void Initialize()
        {
            var triggersToCreate = new List<Trigger>
            {
                new Trigger
                {
                    Id = "GetFrom_Constraint",
                    AltLink = "GetFrom_Constraint",
                    ResourceId = "GetFrom_Constraint",
                    TriggerOperation = TriggerOperation.All,
                    TriggerType = TriggerType.Pre,
                    Body = new DocumentDbUniqueConstraint(new[] { "BucketId", "StreamId", "StartingStreamRevision", "StreamRevision" }, "[GetFromConstraintViolation]").TransformText()
                },
                new Trigger
                {
                    Id = "CheckPoint_Constraint",
                    AltLink = "CheckPoint_Constraint",
                    ResourceId = "CheckPoint_Constraint",
                    TriggerOperation = TriggerOperation.All,
                    TriggerType = TriggerType.Post,
                    Body = new DocumentDbUniqueConstraint(new[] { "CheckpointNumber" }, "[CheckpointConstraintViolation]").TransformText()
                }
            };

            this.EnsureDatabaseExists();
            var collections = this.client.ReadDocumentCollectionFeedAsync(UriFactory.CreateDatabaseUri(this.options.DatabaseName)).GetAwaiter().GetResult();
            this.EnsureCollectionExists(collections, this.options.CommitCollectionName, triggersToCreate);
            this.EnsureCollectionExists(collections, this.options.StreamHeadCollectionName);
            this.EnsureCollectionExists(collections, this.options.SnapshotCollectionName);

            // The reason we're creating one pre and one post trigger here is because of current limitations in the trigger framework as of 04/08/2016.
            // This limitation prevents more than one type of trigger per operation. If more constraints are introduced we'd have to make the T4 template cleverer.
        }

        public ICommit Commit(CommitAttempt attempt)
        {
            Logger.Debug(Messages.AttemptingToCommit, attempt.Events.Count, attempt.StreamId, attempt.CommitSequence, attempt.BucketId);
            return this.TryExecute(() =>
            {
                var doc = attempt.ToDocumentCommit(this.getNextCheckPointNumber());

                while (true)
                {
                    try
                    {
                        var requestOptions = new RequestOptions
                        {
                            PreTriggerInclude = new[] { "GetFrom_Constraint" },
                            PostTriggerInclude = new[] { "CheckPoint_Constraint" }
                        };

                        this.client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.CommitCollectionName), doc, requestOptions).GetAwaiter().GetResult();
                        Logger.Debug(Messages.CommitPersisted, attempt.CommitId, attempt.BucketId);
                        this.SaveStreamHead(attempt.ToDocumentStreamHead());
                        return doc.ToCommit();
                    }
                    catch (DocumentClientException e)
                    {
                        if (e.Message.Contains("[CheckpointConstraintViolation]"))
                        {
                            doc.CheckpointNumber = this.getNextCheckPointNumber().LongValue;
                        }
                        else
                        {
                            var savedCommit = this.LoadSavedCommit(attempt);

                            if (savedCommit != null && savedCommit.CommitId == attempt.CommitId)
                            {
                                throw new DuplicateCommitException();
                            }

                            Logger.Debug(Messages.ConcurrentWriteDetected);
                            throw new ConcurrencyException();
                        }
                    }
                }
            });
        }

        public IEnumerable<ICommit> GetFrom(string checkpointToken = null)
        {
            var checkPoint = LongCheckpoint.Parse(checkpointToken);
            Logger.Debug(Messages.GettingAllCommitsFromCheckpoint, checkPoint.Value);

            return this.TryExecute(() =>
                this.QueryCommits()
                    .Where(s => s.CheckpointNumber > checkPoint.LongValue)
                    .OrderBy(c => c.CheckpointNumber)
                    .ToList()
                    .Select(c => c.ToCommit())
            );
        }

        public IEnumerable<ICommit> GetFrom(string bucketId, string checkpointToken)
        {
            var checkPoint = LongCheckpoint.Parse(checkpointToken);
            Logger.Debug("Getting all commits from Bucket '{0}' and checkpoint '{1}'.", checkPoint.Value);

            return this.TryExecute(() =>
                this.QueryCommits()
                    .Where(s => s.BucketId == bucketId && s.CheckpointNumber > checkPoint.LongValue)
                    .ToList()
                    .Select(c => c.ToCommit())
            );
        }

        public IEnumerable<ICommit> GetFrom(string bucketId, DateTime start)
        {
            Logger.Debug(Messages.GettingAllCommitsFrom, start, bucketId);

            return this.TryExecute(() =>
                this.QueryCommits()
                    .Where(s => s.BucketId == bucketId && s.CommitStamp.Epoch >= start.ToEpoch())
                    .OrderBy(s => s.CheckpointNumber)
                    .ToList()
                    .Select(c => c.ToCommit())
            );
        }

        public IEnumerable<ICommit> GetFromTo(string bucketId, DateTime start, DateTime end)
        {
            Logger.Debug(Messages.GettingAllCommitsFromTo, start, end, bucketId);

            return this.TryExecute(() =>
                    this.QueryCommits()
                        .Where(s => s.BucketId == bucketId && s.CommitStamp.Epoch >= start.ToEpoch() && s.CommitStamp.Epoch <= end.ToEpoch())
                        .ToList()
                        .Select(c => c.ToCommit())
            );
        }

        public IEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            Logger.Debug(Messages.GettingAllCommitsBetween, streamId, bucketId, minRevision, maxRevision);

            return this.TryExecute(() =>
            {
                return this.client.CreateDocumentQuery<DocumentCommit>(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.CommitCollectionName))
                    .Where(s => s.BucketId == bucketId
                                && s.StreamId == streamId
                                && s.StreamRevision >= minRevision
                                && s.StartingStreamRevision <= maxRevision)
                    .OrderBy(s => s.StartingStreamRevision)
                    .ToList()
                    .Select(s => s.ToCommit());
            });
        }

        public ICheckpoint GetCheckpoint(string checkpointToken = null)
        {
            return LongCheckpoint.Parse(checkpointToken);
        }

        public IEnumerable<ICommit> GetUndispatchedCommits()
        {
            Logger.Debug(Messages.GettingUndispatchedCommits);

            return this.TryExecute(() =>
                this.QueryCommits()
                    .Where(s => s.Dispatched == false)
                    .OrderBy(c => c.CheckpointNumber)
                    .ToList()
                    .Select(c => c.ToCommit())
                );
        }

        private void EnsureDatabaseExists()
        {
            var databases = this.client.ReadDatabaseFeedAsync().GetAwaiter().GetResult();
            if (databases.FirstOrDefault(d => d.Id == this.options.DatabaseName) == null)
            {
                this.client.CreateDatabaseAsync(new Database { Id = this.options.DatabaseName }).GetAwaiter().GetResult();
            }
        }

        public void MarkCommitAsDispatched(ICommit commit)
        {
            Logger.Debug(Messages.MarkingCommitAsDispatched, commit.CommitId);

            var documentToUpdate = this.QueryCommits().Where(s => s.Id == commit.ToDocumentCommitId()).ToList().FirstOrDefault();
            if (documentToUpdate == null)
            {
                return;
            }

            documentToUpdate.Dispatched = true;
            this.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.CommitCollectionName, documentToUpdate.Id), documentToUpdate).GetAwaiter().GetResult();
        }

        // This is a very naive way of purging, as it provides no transactional boundary protection.
        // I'll raise an issue to move this to a server-side stored procedure later.
        public void Purge(string bucketId)
        {
			Logger.Warn(Messages.PurgingStorage);

            
            this.TryExecute(() =>
            {
                this.QueryCommits()
                .Where(s => s.BucketId == bucketId)
                .ToList()
                .ForEach(d => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.CommitCollectionName, d.Id)).GetAwaiter().GetResult());

                this.QueryStreamHeads()
                .Where(s => s.BucketId == bucketId)
                .ToList()
                .ForEach(d => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.CommitCollectionName, d.Id)).GetAwaiter().GetResult());

                this.QuerySnapshots()
                .Where(s => s.BucketId == bucketId)
                .ToList()
                .ForEach(d => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.CommitCollectionName, d.Id)).GetAwaiter().GetResult());
            });
        }

        // This is a very naive way of purging, as it provides no transactional boundary protection.
        // I'll raise an issue to move this to a server-side stored procedure later.
        public void Purge()
        {
            this.TryExecute(() =>
            {
                this.QueryCommits()
                .ToList()
                .ForEach(d => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.CommitCollectionName, d.Id)).GetAwaiter().GetResult());

                this.QueryStreamHeads()
                .ToList()
                .ForEach(d => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.StreamHeadCollectionName, d.Id)).GetAwaiter().GetResult());

                this.QuerySnapshots()
                .ToList()
                .ForEach(d => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.SnapshotCollectionName, d.Id)).GetAwaiter().GetResult());
            });
        }

        void IPersistStreams.Drop()
        {
            this.Drop();
        }

        public void Drop()
        {
            this.TryExecute(() =>
            {
                this.client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.CommitCollectionName)).GetAwaiter().GetResult();
                this.client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.SnapshotCollectionName)).GetAwaiter().GetResult();
                this.client.DeleteDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.StreamHeadCollectionName)).GetAwaiter().GetResult();
            });
        }

        public void DeleteStream(string bucketId, string streamId)
        {
            this.TryExecute(() =>
            {
                this.QueryCommits()
                    .Where(s => s.BucketId == bucketId && s.StreamId == streamId)
                    .ToList()
                    .ForEach(s => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.CommitCollectionName, s.Id)).GetAwaiter().GetResult());
                this.QueryStreamHeads()
                    .Where(s => s.BucketId == bucketId && s.StreamId == streamId)
                    .ToList()
                    .ForEach(s => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.StreamHeadCollectionName, s.Id)).GetAwaiter().GetResult());
                this.QuerySnapshots()
                    .Where(s => s.BucketId == bucketId && s.StreamId == streamId)
                    .ToList()
                    .ForEach(s => this.client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.SnapshotCollectionName, s.Id)).GetAwaiter().GetResult());
            });
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        public IEnumerable<IStreamHead> GetStreamsToSnapshot(string bucketId, int maxThreshold)
        {
            Logger.Debug(Messages.GettingStreamsToSnapshot);
            return this.TryExecute(() =>
            {
                return this.QueryStreamHeads()
                           .Where(s => s.SnapshotAge >= maxThreshold)
                           .OrderByDescending(s => s.SnapshotAge)
                           .ToList()
                           .Select(s => s.ToStreamHead());
            });
        }

        public ISnapshot GetSnapshot(string bucketId, string streamId, int maxRevision)
        {
            Logger.Debug(Messages.GettingRevision, streamId, maxRevision);

            return this.TryExecute(() =>
            {
                return this.QuerySnapshots()
                           .Where(s => s.BucketId == bucketId
                                       && s.StreamId == streamId
                                       && s.StreamRevision > 0
                                       && s.StreamRevision <= maxRevision)
                           .OrderByDescending(s => s.SnapshotId)
                           .ToList()
                           .Select(d => d.ToSnapshot())
                           .FirstOrDefault();
            });
        }

        public bool AddSnapshot(ISnapshot snapshot)
        {
            if (snapshot == null)
            {
                return false;
            }

            Logger.Debug(Messages.AddingSnapshot, snapshot.StreamId, snapshot.BucketId, snapshot.StreamRevision);
            try
            {
                var snapshotToUpdate = snapshot.ToDocumentSnapshot();

                this.client.UpsertDocumentAsync(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.SnapshotCollectionName), snapshotToUpdate).GetAwaiter().GetResult();
                var streamHeadToUpdate = this.QueryStreamHeads().Where(s => s.BucketId == snapshot.BucketId && s.StreamId == snapshot.StreamId).ToList().First();
                streamHeadToUpdate.SnapshotRevision = snapshot.StreamRevision;
                this.client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(this.options.DatabaseName, this.options.StreamHeadCollectionName, streamHeadToUpdate.Id), streamHeadToUpdate).GetAwaiter().GetResult();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || this.IsDisposed)
            {
                return;
            }

            Logger.Debug(Messages.ShuttingDownPersistence);
            this.client.Dispose();
            this.IsDisposed = true;
        }

        protected virtual T TryExecute<T>(Func<T> callback)
        {
            T results = default(T);
            this.TryExecute(() => { results = callback(); });
            return results;
        }

        protected virtual void TryExecute(Action callback)
        {
            if (this.IsDisposed)
            {
                throw new ObjectDisposedException("Attempt to use storage after it has been disposed.");
            }

            try
            {
                callback();
            }
            catch (ConcurrencyException)
            {
                throw;
            }
            catch (DuplicateCommitException)
            {
                throw;
            }
            catch (Exception e)
            {
                Logger.Error(Messages.StorageThrewException, e.GetType());
                throw new StorageException(e.Message, e);
            }
        }

        private IOrderedQueryable<DocumentCommit> QueryCommits()
        {
            return this.client.CreateDocumentQuery<DocumentCommit>(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.CommitCollectionName));
        }

        private IOrderedQueryable<DocumentStreamHead> QueryStreamHeads()
        {
            return this.client.CreateDocumentQuery<DocumentStreamHead>(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.StreamHeadCollectionName));
        }

        private IOrderedQueryable<DocumentSnapshot> QuerySnapshots()
        {
            return this.client.CreateDocumentQuery<DocumentSnapshot>(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.SnapshotCollectionName));
        }

        private DocumentCommit LoadSavedCommit(CommitAttempt attempt)
        {
            Logger.Debug(Messages.DetectingConcurrency);

            return this.TryExecute(() =>
            {
                var documentId = attempt.ToDocumentCommitId();
                return this.QueryCommits()
                    .Where(d => d.Id == documentId)
                    .Take(1)
                    .ToList()
                    .FirstOrDefault();
            });
        }

        private void SaveStreamHead(DocumentStreamHead updated)
        {
            this.TryExecute(() =>
            {
                var documents = this.client.ReadDocumentFeedAsync(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.StreamHeadCollectionName)).GetAwaiter().GetResult();
                var documentId = DocumentStreamHead.GetStreamHeadId(updated.BucketId, updated.StreamId);

                DocumentStreamHead streamHead = documents.FirstOrDefault(d => d.Id == documentId) ?? updated;

                streamHead.HeadRevision = updated.HeadRevision;

                if (updated.SnapshotRevision > 0)
                {
                    streamHead.SnapshotRevision = updated.SnapshotRevision;
                }

                return this.client.UpsertDocumentAsync(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.StreamHeadCollectionName), streamHead).Result;
            });
        }

        private void EnsureCollectionExists(FeedResponse<DocumentCollection> collections, string collectionName, List<Trigger> triggersToCreate)
        {
            if (collections.FirstOrDefault(s => s.Id == collectionName) != null) return;

            var collection = new DocumentCollection { Id = collectionName };
            collection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
            this.client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(this.options.DatabaseName), collection).GetAwaiter().GetResult();
            triggersToCreate.ForEach(t => this.client.CreateTriggerAsync(UriFactory.CreateDocumentCollectionUri(this.options.DatabaseName, this.options.CommitCollectionName), t));
        }

        private void EnsureCollectionExists(FeedResponse<DocumentCollection> collections, string collectionName)
        {
            this.EnsureCollectionExists(collections, collectionName, new List<Trigger>());
        }
    }
}