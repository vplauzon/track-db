using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest.Checkpoint
{
    public class CheckpointTest
    {
        private const int MAX_RECORDS = 1000;

        [Fact]
        public async Task NoneLeft()
        {
            var testId = $"CheckpointTest-NoneLeft-{Guid.NewGuid()}";

            await using (var db = await CreateDatabaseAsync(testId))
            {
                var records = Enumerable.Range(0, MAX_RECORDS)
                    .Select(i => new TestDatabase.Workflow(
                        $"My Workflow-{i}",
                        i,
                        TestDatabase.WorkflowState.Started,
                        DateTime.Now));

                db.WorkflowTable.AppendRecords(records);
                //  Delete all records
                db.WorkflowTable.Query().Delete();
            }
            await using (var db = await CreateDatabaseAsync(testId))
            {
                var count = db.WorkflowTable.Query().Count();

                Assert.Equal(0, count);
            }
        }

        [Fact]
        public async Task RemoveHalf()
        {
            var testId = $"CheckpointTest-NoneLeft-{Guid.NewGuid()}";

            await using (var db = await CreateDatabaseAsync(testId))
            {
                var records = Enumerable.Range(0, 2*MAX_RECORDS)
                    .Select(i => new TestDatabase.Workflow(
                        $"My Workflow-{i}",
                        i,
                        TestDatabase.WorkflowState.Started,
                        DateTime.Now));

                db.WorkflowTable.AppendRecords(records);
                //  Delete all records
                db.WorkflowTable.Query().Take(MAX_RECORDS).Delete();
            }
            await using (var db = await CreateDatabaseAsync(testId))
            {
                var count = db.WorkflowTable.Query().Count();

                Assert.Equal(MAX_RECORDS, count);
            }
        }

        private static async Task<TestDatabase> CreateDatabaseAsync(string testId)
        {
            return await TestDatabase.CreateAsync(
                testId,
                p => p with
                {
                    LogPolicy = p.LogPolicy with
                    {
                        MinRecordCountBeforeCheckpoint = MAX_RECORDS
                    }
                });
       }
    }
}