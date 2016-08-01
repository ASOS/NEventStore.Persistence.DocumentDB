// ReSharper disable once CheckNamespace

namespace NEventStore.Persistence.AcceptanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    using NEventStore.Diagnostics;
    using NEventStore.Persistence.AcceptanceTests.BDD;

    using Xunit;
    using Xunit.Should;

    public class when_a_commit_header_has_a_name_that_contains_a_period : PersistenceEngineConcern
    {
        private ICommit persisted;
        private string streamId;

        protected override void Context()
        {
            this.streamId = Guid.NewGuid().ToString();
            var attempt = new CommitAttempt(this.streamId,
                2,
                Guid.NewGuid(),
                1,
                DateTime.Now,
                new Dictionary<string, object> {{"key.1", "value"}},
                new List<EventMessage>
                {
                    new EventMessage {Body = new ExtensionMethods.SomeDomainEvent {SomeProperty = "Test"}}
                });
            this.Persistence.Commit(attempt);
        }

        protected override void Because()
        {
            this.persisted = this.Persistence.GetFrom(this.streamId, 0, int.MaxValue).First();
        }

        [Fact]
        public void should_correctly_deserialize_headers()
        {
            this.persisted.Headers.Keys.ShouldContain("key.1");
        }
    }

    public class when_a_commit_is_successfully_persisted : PersistenceEngineConcern
    {
        private CommitAttempt attempt;
        private DateTime now;
        private ICommit persisted;
        private string streamId;

        protected override void Context()
        {
            this.now = SystemTime.UtcNow.AddYears(1);
            this.streamId = Guid.NewGuid().ToString();
            this.attempt = this.streamId.BuildAttempt(this.now);

            this.Persistence.Commit(this.attempt);
        }

        protected override void Because()
        {
            this.persisted = this.Persistence.GetFrom(this.streamId, 0, int.MaxValue).First();
        }

        [Fact]
        public void should_correctly_persist_the_stream_identifier()
        {
            this.persisted.StreamId.ShouldBe(this.attempt.StreamId);
        }

        [Fact]
        public void should_correctly_persist_the_stream_stream_revision()
        {
            this.persisted.StreamRevision.ShouldBe(this.attempt.StreamRevision);
        }

        [Fact]
        public void should_correctly_persist_the_commit_identifier()
        {
            this.persisted.CommitId.ShouldBe(this.attempt.CommitId);
        }

        [Fact]
        public void should_correctly_persist_the_commit_sequence()
        {
            this.persisted.CommitSequence.ShouldBe(this.attempt.CommitSequence);
        }

        // persistence engines have varying levels of precision with respect to time.
        [Fact]
        public void should_correctly_persist_the_commit_stamp()
        {
            var difference = this.persisted.CommitStamp.Subtract(this.now);
            difference.Days.ShouldBe(0);
            difference.Hours.ShouldBe(0);
            difference.Minutes.ShouldBe(0);
            difference.ShouldBeLessThanOrEqualTo(TimeSpan.FromSeconds(1));
        }

        [Fact]
        public void should_correctly_persist_the_headers()
        {
            this.persisted.Headers.Count.ShouldBe(this.attempt.Headers.Count);
        }

        [Fact]
        public void should_correctly_persist_the_events()
        {
            this.persisted.Events.Count.ShouldBe(this.attempt.Events.Count);
        }

        [Fact]
        public void should_add_the_commit_to_the_set_of_undispatched_commits()
        {
            this.Persistence.GetUndispatchedCommits()
                .FirstOrDefault(x => x.CommitId == this.attempt.CommitId)
                .ShouldNotBeNull();
        }

        [Fact]
        public void should_cause_the_stream_to_be_found_in_the_list_of_streams_to_snapshot()
        {
            this.Persistence.GetStreamsToSnapshot(1).FirstOrDefault(x => x.StreamId == this.streamId).ShouldNotBeNull();
        }
    }

    public class when_reading_from_a_given_revision_to_commit_revision : PersistenceEngineConcern
    {
        private const int LoadFromCommitContainingRevision = 3;
        private const int UpToCommitWithContainingRevision = 6;
        private ICommit[] committed;
        private ICommit oldest, oldest2, oldest3;
        private string streamId;

        protected override void Context()
        {
            this.oldest = this.Persistence.CommitSingle(); // 2 events, revision 1-2
            this.oldest2 = this.Persistence.CommitNext(this.oldest); // 2 events, revision 3-4
            this.oldest3 = this.Persistence.CommitNext(this.oldest2); // 2 events, revision 5-6
            this.Persistence.CommitNext(this.oldest3); // 2 events, revision 7-8

            this.streamId = this.oldest.StreamId;
        }

        protected override void Because()
        {
            this.committed =
                this.Persistence.GetFrom(this.streamId, LoadFromCommitContainingRevision,
                    UpToCommitWithContainingRevision).ToArray();
        }

        [Fact]
        public void should_start_from_the_commit_which_contains_the_min_stream_revision_specified()
        {
            this.committed.First().CommitId.ShouldBe(this.oldest2.CommitId); // contains revision 3
        }

        [Fact]
        public void should_read_up_to_the_commit_which_contains_the_max_stream_revision_specified()
        {
            this.committed.Last().CommitId.ShouldBe(this.oldest3.CommitId); // contains revision 6
        }
    }

    public class when_committing_a_stream_with_the_same_revision : PersistenceEngineConcern
    {
        private CommitAttempt attemptWithSameRevision;
        private Exception thrown;

        protected override void Context()
        {
            var commit = this.Persistence.CommitSingle();
            this.attemptWithSameRevision = commit.StreamId.BuildAttempt();
        }

        protected override void Because()
        {
            this.thrown = Catch.Exception(() => this.Persistence.Commit(this.attemptWithSameRevision));
        }

        [Fact]
        public void should_throw_a_ConcurrencyException()
        {
            this.thrown.ShouldBeInstanceOf<ConcurrencyException>();
        }
    }

    public class when_attempting_to_persist_a_commit_twice : PersistenceEngineConcern
    {
        private CommitAttempt attemptTwice;
        private Exception thrown;

        protected override void Context()
        {
            var commit = this.Persistence.CommitSingle();
            this.attemptTwice = new CommitAttempt(
                commit.BucketId,
                commit.StreamId,
                commit.StreamRevision,
                commit.CommitId,
                commit.CommitSequence,
                commit.CommitStamp,
                commit.Headers,
                commit.Events);
        }

        protected override void Because()
        {
            this.thrown = Catch.Exception(() => this.Persistence.Commit(this.attemptTwice));
        }

        [Fact]
        public void should_throw_a_DuplicateCommitException()
        {
            this.thrown.ShouldBeInstanceOf<DuplicateCommitException>();
        }
    }

    public class when_a_commit_has_been_marked_as_dispatched : PersistenceEngineConcern
    {
        private ICommit commit;

        protected override void Context()
        {
            this.commit = this.Persistence.CommitSingle();
        }

        protected override void Because()
        {
            this.Persistence.MarkCommitAsDispatched(this.commit);
        }

        [Fact]
        public void should_no_longer_be_found_in_the_set_of_undispatched_commits()
        {
            this.Persistence.GetUndispatchedCommits()
                .FirstOrDefault(x => x.CommitId == this.commit.CommitId)
                .ShouldBeNull();
        }
    }

    public class when_saving_a_snapshot : PersistenceEngineConcern
    {
        private bool added;
        private Snapshot snapshot;
        private string streamId;

        protected override void Context()
        {
            this.streamId = Guid.NewGuid().ToString();
            this.snapshot = new Snapshot(this.streamId, 1, "Snapshot");
            this.Persistence.CommitSingle(this.streamId);
        }

        protected override void Because()
        {
            this.added = this.Persistence.AddSnapshot(this.snapshot);
        }

        [Fact]
        public void should_indicate_the_snapshot_was_added()
        {
            this.added.ShouldBeTrue();
        }

        [Fact]
        public void should_be_able_to_retrieve_the_snapshot()
        {
            this.Persistence.GetSnapshot(this.streamId, this.snapshot.StreamRevision).ShouldNotBeNull();
        }
    }

    public class when_retrieving_a_snapshot : PersistenceEngineConcern
    {
        private ISnapshot correct;
        private ISnapshot snapshot;
        private string streamId;
        private ISnapshot tooFarForward;

        protected override void Context()
        {
            this.streamId = Guid.NewGuid().ToString();
            var commit1 = this.Persistence.CommitSingle(this.streamId); // rev 1-2
            var commit2 = this.Persistence.CommitNext(commit1); // rev 3-4
            this.Persistence.CommitNext(commit2); // rev 5-6

            this.Persistence.AddSnapshot(new Snapshot(this.streamId, 1, string.Empty)); //Too far back
            this.Persistence.AddSnapshot(this.correct = new Snapshot(this.streamId, 3, "Snapshot"));
            this.Persistence.AddSnapshot(this.tooFarForward = new Snapshot(this.streamId, 5, string.Empty));
        }

        protected override void Because()
        {
            this.snapshot = this.Persistence.GetSnapshot(this.streamId, this.tooFarForward.StreamRevision - 1);
        }

        [Fact]
        public void should_load_the_most_recent_prior_snapshot()
        {
            this.snapshot.StreamRevision.ShouldBe(this.correct.StreamRevision);
        }

        [Fact]
        public void should_have_the_correct_snapshot_payload()
        {
            this.snapshot.Payload.ShouldBe(this.correct.Payload);
        }

        [Fact]
        public void should_have_the_correct_stream_id()
        {
            this.snapshot.StreamId.ShouldBe(this.correct.StreamId);
        }
    }

    public class when_a_snapshot_has_been_added_to_the_most_recent_commit_of_a_stream : PersistenceEngineConcern
    {
        private const string SnapshotData = "snapshot";
        private ICommit newest;
        private ICommit _oldest, _oldest2;
        private string _streamId;

        protected override void Context()
        {
            this._streamId = Guid.NewGuid().ToString();
            this._oldest = this.Persistence.CommitSingle(this._streamId);
            this._oldest2 = this.Persistence.CommitNext(this._oldest);
            this.newest = this.Persistence.CommitNext(this._oldest2);
        }

        protected override void Because()
        {
            this.Persistence.AddSnapshot(new Snapshot(this._streamId, this.newest.StreamRevision, SnapshotData));
        }

        [Fact]
        public void should_no_longer_find_the_stream_in_the_set_of_streams_to_be_snapshot()
        {
            this.Persistence.GetStreamsToSnapshot(1).Any(x => x.StreamId == this._streamId).ShouldBeFalse();
        }
    }

    public class when_adding_a_commit_after_a_snapshot : PersistenceEngineConcern
    {
        private const int WithinThreshold = 2;
        private const int OverThreshold = 3;
        private const string SnapshotData = "snapshot";
        private ICommit oldest, oldest2;
        private string streamId;

        protected override void Context()
        {
            this.streamId = Guid.NewGuid().ToString();
            this.oldest = this.Persistence.CommitSingle(this.streamId);
            this.oldest2 = this.Persistence.CommitNext(this.oldest);
            this.Persistence.AddSnapshot(new Snapshot(this.streamId, this.oldest2.StreamRevision, SnapshotData));
        }

        protected override void Because()
        {
            this.Persistence.Commit(this.oldest2.BuildNextAttempt());
        }

        // Because Raven and Mongo update the stream head asynchronously, occasionally will fail this test
        [Fact]
        public void should_find_the_stream_in_the_set_of_streams_to_be_snapshot_when_within_the_threshold()
        {
            this.Persistence.GetStreamsToSnapshot(WithinThreshold)
                .FirstOrDefault(x => x.StreamId == this.streamId)
                .ShouldNotBeNull();
        }

        [Fact]
        public void should_not_find_the_stream_in_the_set_of_streams_to_be_snapshot_when_over_the_threshold()
        {
            this.Persistence.GetStreamsToSnapshot(OverThreshold).Any(x => x.StreamId == this.streamId).ShouldBeFalse();
        }
    }

    public class when_reading_all_commits_from_a_particular_point_in_time : PersistenceEngineConcern
    {
        private ICommit[] committed;
        private CommitAttempt first;
        private DateTime now;
        private ICommit second;
        private string streamId;
        private ICommit third;

        protected override void Context()
        {
            this.streamId = Guid.NewGuid().ToString();

            this.now = SystemTime.UtcNow.AddYears(1);
            this.first = this.streamId.BuildAttempt(this.now.AddSeconds(1));
            this.Persistence.Commit(this.first);

            this.second = this.Persistence.CommitNext(this.first);
            this.third = this.Persistence.CommitNext(this.second);
            this.Persistence.CommitNext(this.third);
        }

        protected override void Because()
        {
            this.committed = this.Persistence.GetFrom(this.now).ToArray();
        }

        [Fact]
        public void should_return_all_commits_on_or_after_the_point_in_time_specified()
        {
            this.committed.Length.ShouldBe(4);
        }
    }

    public class when_reading_all_commits_from_the_year_1_AD : PersistenceEngineConcern
    {
        private Exception thrown;

        protected override void Because()
        {
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            this.thrown = Catch.Exception(() => this.Persistence.GetFrom(DateTime.MinValue).FirstOrDefault());
        }

        [Fact]
        public void should_NOT_throw_an_exception()
        {
            this.thrown.ShouldBeNull();
        }
    }

    public class when_purging_all_commits : PersistenceEngineConcern
    {
        protected override void Context()
        {
            this.Persistence.CommitSingle();
        }

        protected override void Because()
        {
            this.Persistence.Purge();
        }

        [Fact]
        public void should_not_find_any_commits_stored()
        {
            this.Persistence.GetFrom(DateTime.MinValue).Count().ShouldBe(0);
        }

        [Fact]
        public void should_not_find_any_streams_to_snapshot()
        {
            this.Persistence.GetStreamsToSnapshot(0).Count().ShouldBe(0);
        }

        [Fact]
        public void should_not_find_any_undispatched_commits()
        {
            this.Persistence.GetUndispatchedCommits().Count().ShouldBe(0);
        }
    }

    public class when_invoking_after_disposal : PersistenceEngineConcern
    {
        private Exception thrown;

        protected override void Context()
        {
            this.Persistence.Dispose();
        }

        protected override void Because()
        {
            this.thrown = Catch.Exception(() => this.Persistence.CommitSingle());
        }

        [Fact]
        public void should_throw_an_ObjectDisposedException()
        {
            this.thrown.ShouldBeInstanceOf<ObjectDisposedException>();
        }
    }

    public class when_committing_a_stream_with_the_same_id_as_a_stream_same_bucket : PersistenceEngineConcern
    {
        private string streamId;
        private static Exception thrown;
        private DateTime attemptACommitStamp;

        protected override void Context()
        {
            this.streamId = Guid.NewGuid().ToString();
            this.Persistence.Commit(this.streamId.BuildAttempt());
        }

        protected override void Because()
        {
            thrown = Catch.Exception(() => this.Persistence.Commit(this.streamId.BuildAttempt()));
        }

        [Fact]
        public void should_throw()
        {
            thrown.ShouldNotBeNull();
        }

        [Fact]
        public void should_be_duplicate_commit_exception()
        {
            thrown.ShouldBeInstanceOf<ConcurrencyException>();
        }
    }

    public class when_committing_a_stream_with_the_same_id_as_a_stream_in_another_bucket : PersistenceEngineConcern
    {
        private const string _bucketAId = "a";
        private const string _bucketBId = "b";
        private string _streamId;
        private static CommitAttempt _attemptForBucketB;
        private static Exception _thrown;
        private DateTime _attemptACommitStamp;

        protected override void Context()
        {
            this._streamId = Guid.NewGuid().ToString();
            var now = SystemTime.UtcNow;
            this.Persistence.Commit(this._streamId.BuildAttempt(now, _bucketAId));
            this._attemptACommitStamp =
                this.Persistence.GetFrom(_bucketAId, this._streamId, 0, int.MaxValue).First().CommitStamp;
            _attemptForBucketB = this._streamId.BuildAttempt(now.Subtract(TimeSpan.FromDays(1)), _bucketBId);
        }

        protected override void Because()
        {
            _thrown = Catch.Exception(() => this.Persistence.Commit(_attemptForBucketB));
        }

        [Fact]
        public void should_succeed()
        {
            _thrown.ShouldBeNull();
        }

        [Fact]
        public void should_persist_to_the_correct_bucket()
        {
            var stream = this.Persistence.GetFrom(_bucketBId, this._streamId, 0, int.MaxValue).ToArray();
            stream.ShouldNotBeNull();
            stream.Count().ShouldBe(1);
        }

        [Fact]
        public void should_not_affect_the_stream_from_the_other_bucket()
        {
            var stream = this.Persistence.GetFrom(_bucketAId, this._streamId, 0, int.MaxValue).ToArray();
            stream.ShouldNotBeNull();
            stream.Count().ShouldBe(1);
            stream.First().CommitStamp.ShouldBe(this._attemptACommitStamp);
        }
    }

    public class when_saving_a_snapshot_for_a_stream_with_the_same_id_as_a_stream_in_another_bucket
        : PersistenceEngineConcern
    {
        private const string _bucketAId = "a";
        private const string _bucketBId = "b";

        private string _streamId;

        private static Snapshot _snapshot;

        protected override void Context()
        {
            this._streamId = Guid.NewGuid().ToString();
            _snapshot = new Snapshot(_bucketBId, this._streamId, 1, "Snapshot");
            this.Persistence.Commit(this._streamId.BuildAttempt(bucketId: _bucketAId));
            this.Persistence.Commit(this._streamId.BuildAttempt(bucketId: _bucketBId));
        }

        protected override void Because()
        {
            this.Persistence.AddSnapshot(_snapshot);
        }

        [Fact]
        public void should_affect_snapshots_from_another_bucket()
        {
            this.Persistence.GetSnapshot(_bucketAId, this._streamId, _snapshot.StreamRevision).ShouldBeNull();
        }
    }

    public class when_reading_all_commits_from_a_particular_point_in_time_and_there_are_streams_in_multiple_buckets
        : PersistenceEngineConcern
    {
        private const string _bucketAId = "a";
        private const string _bucketBId = "b";

        private static DateTime _now;
        private static ICommit[] _returnedCommits;
        private CommitAttempt _commitToBucketB;

        protected override void Context()
        {
            _now = SystemTime.UtcNow.AddYears(1);

            var commitToBucketA = Guid.NewGuid().ToString().BuildAttempt(_now.AddSeconds(1), _bucketAId);

            this.Persistence.Commit(commitToBucketA);
            this.Persistence.Commit(commitToBucketA = commitToBucketA.BuildNextAttempt());
            this.Persistence.Commit(commitToBucketA = commitToBucketA.BuildNextAttempt());
            this.Persistence.Commit(commitToBucketA.BuildNextAttempt());

            this._commitToBucketB = Guid.NewGuid().ToString().BuildAttempt(_now.AddSeconds(1), _bucketBId);

            this.Persistence.Commit(this._commitToBucketB);
        }

        protected override void Because()
        {
            _returnedCommits = this.Persistence.GetFrom(_bucketAId, _now).ToArray();
        }

        [Fact]
        public void should_not_return_commits_from_other_buckets()
        {
            _returnedCommits.Any(c => c.CommitId.Equals(this._commitToBucketB.CommitId)).ShouldBeFalse();
        }
    }

    public class when_getting_all_commits_since_checkpoint_and_there_are_streams_in_multiple_buckets
        : PersistenceEngineConcern
    {
        private ICommit[] _commits;

        protected override void Context()
        {
            const string bucketAId = "a";
            const string bucketBId = "b";
            this.Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketAId));
            this.Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketBId));
            this.Persistence.Commit(Guid.NewGuid().ToString().BuildAttempt(bucketId: bucketAId));
        }

        protected override void Because()
        {
            this._commits = this.Persistence.GetFromStart().ToArray();
        }

        [Fact]
        public void should_not_be_empty()
        {
            this._commits.ShouldNotBeEmpty();
        }

        [Fact]
        public void should_be_in_order_by_checkpoint()
        {
            var checkpoint = this.Persistence.GetCheckpoint();
            foreach (var commit in this._commits)
            {
                var commitCheckpoint = this.Persistence.GetCheckpoint(commit.CheckpointToken);
                commitCheckpoint.ShouldBeGreaterThan(checkpoint);
                checkpoint = this.Persistence.GetCheckpoint(commit.CheckpointToken);
            }
        }
    }

    public class when_purging_all_commits_and_there_are_streams_in_multiple_buckets : PersistenceEngineConcern
    {
        private const string _bucketAId = "a";
        private const string _bucketBId = "b";

        private string _streamId;

        protected override void Context()
        {
            this._streamId = Guid.NewGuid().ToString();
            this.Persistence.Commit(this._streamId.BuildAttempt(bucketId: _bucketAId));
            this.Persistence.Commit(this._streamId.BuildAttempt(bucketId: _bucketBId));
        }

        protected override void Because()
        {
            this.Persistence.Purge();
        }

        [Fact]
        public void should_purge_all_commits_stored_in_bucket_a()
        {
            this.Persistence.GetFrom(_bucketAId, DateTime.MinValue).Count().ShouldBe(0);
        }

        [Fact]
        public void should_purge_all_commits_stored_in_bucket_b()
        {
            this.Persistence.GetFrom(_bucketBId, DateTime.MinValue).Count().ShouldBe(0);
        }

        [Fact]
        public void should_purge_all_streams_to_snapshot_in_bucket_a()
        {
            this.Persistence.GetStreamsToSnapshot(_bucketAId, 0).Count().ShouldBe(0);
        }

        [Fact]
        public void should_purge_all_streams_to_snapshot_in_bucket_b()
        {
            this.Persistence.GetStreamsToSnapshot(_bucketBId, 0).Count().ShouldBe(0);
        }

        [Fact]
        public void should_purge_all_undispatched_commits()
        {
            this.Persistence.GetUndispatchedCommits().Count().ShouldBe(0);
        }
    }

    public class PersistenceEngineConcern : SpecificationBase, IUseFixture<PersistenceEngineFixture>
    {
        private PersistenceEngineFixture _fixture;

        protected IPersistStreams Persistence
        {
            get { return this._fixture.Persistence; }
        }

        protected void Reinitialize()
        {
            this._fixture.Initialize();
        }

        public void SetFixture(PersistenceEngineFixture data)
        {
            this._fixture = data;
            this._fixture.Initialize();
        }
    }

    public partial class PersistenceEngineFixture : IDisposable
    {
        private readonly Func<IPersistStreams> createPersistence;

        public void Initialize()
        {
            if (this.Persistence != null && !this.Persistence.IsDisposed)
            {
                this.Persistence.Drop();
                this.Persistence.Dispose();
            }

            this.Persistence = new PerformanceCounterPersistenceEngine(this.createPersistence(), "tests");
            this.Persistence.Initialize();
        }

        public IPersistStreams Persistence { get; private set; }

        public void Dispose()
        {
            if (this.Persistence != null && !this.Persistence.IsDisposed)
            {
                this.Persistence.Drop();
                Thread.Sleep(500);
                this.Persistence.Dispose();
            }
        }
    }
}
