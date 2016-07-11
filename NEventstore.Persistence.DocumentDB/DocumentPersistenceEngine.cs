namespace NEventStore.Persistence.DocumentDB
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    using NEventStore.Logging;
    using NEventStore.Serialization;

    public class DocumentPersistenceEngine : IPersistStreams
    {
        private static readonly ILog Logger = LogFactory.BuildLogger(typeof(DocumentPersistenceEngine));
        private bool disposed;

        private readonly Func<long> getLastCheckPointNumber;
        private readonly Func<LongCheckpoint> getNextCheckPointNumber;
        private readonly LongCheckpoint checkPointZero;

        public DocumentPersistenceEngine(DocumentClient client, DocumentPersistenceOptions options)
        {
            this.Client = client;
            this.Options = options;

            this.getLastCheckPointNumber = () => this.TryExecute(() =>
            {
                var max = this.QueryCommits().OrderByDescending(s => s.CheckpointNumber).Take(1).ToList().FirstOrDefault();

                return max != null ? max.CheckpointNumber : 0L;
            });

            this.getNextCheckPointNumber = () => new LongCheckpoint(this.getLastCheckPointNumber() + 1L);
            this.checkPointZero = new LongCheckpoint(0);
        }

        private IOrderedQueryable<DocumentCommit> QueryCommits()
        {
            return this.Client.CreateDocumentQuery<DocumentCommit>(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.CommitCollectionName));
        }

        private IOrderedQueryable<DocumentStreamHead> QueryStreamHeads()
        {
            return this.Client.CreateDocumentQuery<DocumentStreamHead>(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.StreamHeadCollectionName));
        }

        private IOrderedQueryable<DocumentSnapshot> QuerySnapshots()
        {
            return this.Client.CreateDocumentQuery<DocumentSnapshot>(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.SnapshotCollectionName));
        }

        public DocumentClient Client { get; private set; }

        public IDocumentSerializer Serializer { get; private set; }

        public DocumentPersistenceOptions Options { get; private set; }

        public bool IsDisposed
        {
            get { return this.disposed; }
        }

        public void Initialize()
        {
            this.EnsureDatabaseExists();
            var collections = this.Client.ReadDocumentCollectionFeedAsync(UriFactory.CreateDatabaseUri(this.Options.DatabaseName)).GetAwaiter().GetResult();
            this.EnsureCollectionExists(collections, this.Options.CommitCollectionName);
            this.EnsureCollectionExists(collections, this.Options.StreamHeadCollectionName);
            this.EnsureCollectionExists(collections, this.Options.SnapshotCollectionName);

            var logicalKeyConstraint = new Trigger
            {
                Id = "LogicalKey_Constraint",
                AltLink = "LogicalKey_Constraint",
                ResourceId = "LogicalKey_Constraint",
                TriggerOperation = TriggerOperation.Create,
                TriggerType = TriggerType.Pre,
                Body = UniqueConstraintScripts.Logical_Key
            };

            this.Client.CreateTriggerAsync(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.CommitCollectionName), logicalKeyConstraint, new RequestOptions());
        }

        public void Drop()
        {
            this.Purge();
        }

        public void DeleteStream(string bucketId, string streamId)
        {
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

        public ICheckpoint GetCheckpoint(string checkpointToken = null)
        {
            return LongCheckpoint.Parse(checkpointToken);
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
            var databases = this.Client.ReadDatabaseFeedAsync().GetAwaiter().GetResult();
            if (databases.FirstOrDefault(d => d.Id == this.Options.DatabaseName) == null)
            {
                this.Client.CreateDatabaseAsync(new Database { Id = this.Options.DatabaseName }).GetAwaiter().GetResult();
            }
        }

        private void EnsureCollectionExists(FeedResponse<DocumentCollection> collections, string collectionName)
        {
            if (collections.FirstOrDefault(s => s.Id == collectionName) == null)
            {
                this.Client.CreateDocumentCollectionAsync(UriFactory.CreateDatabaseUri(this.Options.DatabaseName), new DocumentCollection { Id = collectionName }).GetAwaiter().GetResult();
            }
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

        public void MarkCommitAsDispatched(ICommit commit)
        {
            Logger.Debug(Messages.MarkingCommitAsDispatched, commit.CommitId);

            var documentToUpdate = this.QueryCommits().Where(s => s.Id == commit.ToDocumentCommitId()).ToList().FirstOrDefault();
            if (documentToUpdate == null)
            {
                return;
            }

            documentToUpdate.Dispatched = true;
            this.Client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(this.Options.DatabaseName, this.Options.CommitCollectionName, documentToUpdate.Id), documentToUpdate).GetAwaiter().GetResult();
        }

        public void Purge(string bucketId)
        {
            Logger.Warn(Messages.PurgingStorage);

        }

        void IPersistStreams.Drop()
        {
            this.Drop();
        }

        public void Purge()
        {
            this.TryExecute(() =>
            {
                var commitsToDelete = this.QueryCommits().ToList();
                commitsToDelete.ForEach(d => this.Client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.Options.DatabaseName, this.Options.CommitCollectionName, d.Id)).GetAwaiter().GetResult());

                var streamHeadersToDelete = this.QueryStreamHeads().ToList();
                streamHeadersToDelete.ForEach(d => this.Client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.Options.DatabaseName, this.Options.StreamHeadCollectionName, d.Id)).GetAwaiter().GetResult());

                var snapshotsToDelete = this.QuerySnapshots().ToList();
                snapshotsToDelete.ForEach(d => this.Client.DeleteDocumentAsync(UriFactory.CreateDocumentUri(this.Options.DatabaseName, this.Options.SnapshotCollectionName, d.Id)).GetAwaiter().GetResult());
            });
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing || this.disposed)
            {
                return;
            }

            Logger.Debug(Messages.ShuttingDownPersistence);
            this.Client.Dispose();
            this.disposed = true;
        }

        public ICommit Commit(CommitAttempt attempt)
        {
            Logger.Debug(Messages.AttemptingToCommit, attempt.Events.Count, attempt.StreamId, attempt.CommitSequence, attempt.BucketId);
            return this.TryExecute(() =>
            {
                try
                {
                    var doc = attempt.ToDocumentCommit(this.getNextCheckPointNumber());
                    var requestOptions = new RequestOptions
                    {
                        PreTriggerInclude = new List<string>
                            {
                                "LogicalKey_Constraint"
                            }
                    };

                    this.Client.CreateDocumentAsync(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.CommitCollectionName), doc, requestOptions).GetAwaiter().GetResult();

                    Logger.Debug(Messages.CommitPersisted, attempt.CommitId, attempt.BucketId);
                    this.SaveStreamHead(attempt.ToDocumentStreamHead());

                    return doc.ToCommit();
                }
                catch (DocumentClientException)
                {
                    var savedCommit = this.LoadSavedCommit(attempt);

                    if (savedCommit != null && savedCommit.CommitId == attempt.CommitId)
                    {
                        throw new DuplicateCommitException();
                    }

                    Logger.Debug(Messages.ConcurrentWriteDetected);
                    throw new ConcurrencyException();
                }
            });
        }

        public IEnumerable<ICommit> GetFrom(string bucketId, string streamId, int minRevision, int maxRevision)
        {
            Logger.Debug(Messages.GettingAllCommitsBetween, streamId, bucketId, minRevision, maxRevision);

            return this.TryExecute(() =>
            {
                var documents = this.Client.CreateDocumentQuery<DocumentCommit>(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.CommitCollectionName))
                    .Where(s => s.BucketId == bucketId && s.StreamId == streamId && s.StreamRevision >= minRevision && s.StartingStreamRevision <= maxRevision).ToList();

                return documents.OrderBy(s => s.StartingStreamRevision).Select(s => s.ToCommit());
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

                this.Client.UpsertDocumentAsync(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.SnapshotCollectionName), snapshotToUpdate).GetAwaiter().GetResult();
                var streamHeadToUpdate = this.QueryStreamHeads().Where(s => s.BucketId == snapshot.BucketId && s.StreamId == snapshot.StreamId).ToList().First();
                streamHeadToUpdate.SnapshotRevision = snapshot.StreamRevision;
                this.Client.ReplaceDocumentAsync(UriFactory.CreateDocumentUri(this.Options.DatabaseName, this.Options.StreamHeadCollectionName, streamHeadToUpdate.Id), streamHeadToUpdate).GetAwaiter().GetResult();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
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

        protected virtual T TryExecute<T>(Func<T> callback)
        {
            T results = default(T);
            this.TryExecute(() => { results = callback(); });
            return results;
        }

        protected virtual void TryExecute(Action callback)
        {
            if (this.disposed)
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

        private DocumentCommit LoadSavedCommit(CommitAttempt attempt)
        {
            Logger.Debug(Messages.DetectingConcurrency);

            return this.TryExecute(() =>
            {
                var documentId = attempt.ToDocumentCommitId();
                return this.QueryCommits()
                    .Where(d => d.Id == documentId)
                    .ToList()
                    .FirstOrDefault();
            });
        }

        private void SaveStreamHead(DocumentStreamHead updated)
        {
            this.TryExecute(() =>
            {
                var documents = this.Client.ReadDocumentFeedAsync(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.StreamHeadCollectionName)).GetAwaiter().GetResult();
                var documentId = DocumentStreamHead.GetStreamHeadId(updated.BucketId, updated.StreamId);

                DocumentStreamHead streamHead = documents.FirstOrDefault(d => d.Id == documentId) ?? updated;

                streamHead.HeadRevision = updated.HeadRevision;

                if (updated.SnapshotRevision > 0)
                {
                    streamHead.SnapshotRevision = updated.SnapshotRevision;
                }

                return this.Client.UpsertDocumentAsync(UriFactory.CreateDocumentCollectionUri(this.Options.DatabaseName, this.Options.StreamHeadCollectionName), streamHead).Result;
            });
        }
    }
}