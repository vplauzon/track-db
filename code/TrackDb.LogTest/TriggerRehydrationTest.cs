using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest
{
    public class TriggerRehydrationTest : TestBase
    {
        [Fact]
        public async Task Test00010By01()
        {
            await RunPerformanceTestAsync(GetTestId(), 10, 1);
        }

        [Fact]
        public async Task Test00100By01()
        {
            await RunPerformanceTestAsync(GetTestId(), 100, 1);
        }

        [Fact]
        public async Task Test01000By01()
        {
            await RunPerformanceTestAsync(GetTestId(), 1000, 1);
        }

        [Fact]
        public async Task Test01000By10()
        {
            await RunPerformanceTestAsync(GetTestId(), 1000, 10);
        }

        [Fact]
        public async Task Test10000By10()
        {
            await RunPerformanceTestAsync(GetTestId(), 10000, 10);
        }

        private async Task RunPerformanceTestAsync(string testId, int recordCount, int batchSize)
        {
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                SetupData(db, recordCount, batchSize);
                Transition(
                    db,
                    TestDatabase.WorkflowState.Pending,
                    TestDatabase.WorkflowState.Started,
                    batchSize);

                ValidateData(db, TestDatabase.WorkflowState.Started, recordCount);
            }
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                ValidateData(db, TestDatabase.WorkflowState.Started, recordCount);
                Transition(
                    db,
                    TestDatabase.WorkflowState.Started,
                    TestDatabase.WorkflowState.Completed,
                    batchSize);
                ValidateData(db, TestDatabase.WorkflowState.Completed, recordCount);
            }
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                ValidateData(db, TestDatabase.WorkflowState.Completed, recordCount);
            }
        }

        private void SetupData(TestDatabase db, int recordCount, int batchSize)
        {
            for (var i = 0; i != recordCount / batchSize; ++i)
            {
                var records = Enumerable.Range(i * batchSize, batchSize)
                    .Select(i => new TestDatabase.Workflow(
                        $"Workflow-{i}",
                        i,
                        false,
                        TestDatabase.WorkflowState.Pending,
                        DateTime.Now));

                db.WorkflowTable.AppendRecords(records);
            }
        }

        private void Transition(
            TestDatabase db,
            TestDatabase.WorkflowState fromState,
            TestDatabase.WorkflowState toState,
            int batchSize)
        {
            while (true)
            {
                using (var tx = db.CreateTransaction())
                {
                    var workflows = db.WorkflowTable.Query(tx)
                        .Where(pf => pf.Equal(w => w.State, fromState))
                        .Take(batchSize)
                        .ToImmutableArray();

                    if (workflows.Length == 0)
                    {
                        return;
                    }
                    foreach (var w in workflows)
                    {
                        db.WorkflowTable.UpdateRecord(
                            w,
                            w with { State = toState },
                            tx);
                    }

                    tx.Complete();
                }
            }
        }

        private static void ValidateData(
            TestDatabase db,
            TestDatabase.WorkflowState filledState,
            int recordCount)
        {
            Assert.Equal(
                recordCount,
                db.WorkflowTable.Query()
                .Where(pf => pf.Equal(w => w.State, filledState))
                .Count());
            Assert.Equal(
                recordCount,
                db.WorkflowSummaryTable.Query()
                .Where(pf => pf.Equal(w => w.State, filledState))
                .Sum(ws => ws.WorkflowCount));

            foreach (var state in Enum.GetValues<TestDatabase.WorkflowState>())
            {
                if (state != filledState)
                {
                    Assert.Equal(
                        0,
                        db.WorkflowTable.Query()
                        .Where(pf => pf.Equal(w => w.State, state))
                        .Count());
                    Assert.Equal(
                        0,
                        db.WorkflowSummaryTable.Query()
                        .Where(pf => pf.Equal(w => w.State, state))
                        .Sum(ws => ws.WorkflowCount));
                }
            }
        }
    }
}