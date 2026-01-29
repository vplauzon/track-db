using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest.Volume
{
    public class UpdateSinglesTest : TestBase
    {
        [Fact]
        public async Task Test00010()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 10);
        }

        [Fact]
        public async Task Test00050()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 50);
        }

        [Fact]
        public async Task Test00250()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 250);
        }

        [Fact]
        public async Task Test01000()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 1000);
        }

        [Fact]
        public async Task ReloadTest01000()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 1000, 100);
        }

        [Fact]
        public async Task Test010000()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 10000);
        }

        private async Task RunPerformanceTestAsync(
            string testId,
            long cycleCount,
            int reloadCycleCount = 0)
        {
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            {
                var db = await TestDatabase.CreateAsync(testId);

                for (int i = 0; i != cycleCount; ++i)
                {
                    var workflow = new TestDatabase.Workflow(
                        $"Workflow-{i}",
                        i,
                        TestDatabase.WorkflowState.Pending,
                        DateTime.Now);
                    var activity1 = new TestDatabase.Activity(
                        workflow.WorkflowName,
                        $"Activity-{i}-1",
                        null,
                        TestDatabase.ActivityState.Started);
                    var task11 = new TestDatabase.ActivityTask(
                        workflow.WorkflowName,
                        activity1.ActivityName,
                        $"Task-{i}-11",
                        TestDatabase.TaskState.Started,
                        DateTime.Now,
                        null);
                    var task12 = new TestDatabase.ActivityTask(
                        workflow.WorkflowName,
                        activity1.ActivityName,
                        $"Task-{i}-12",
                        TestDatabase.TaskState.Started,
                        task11.StartTime,
                        DateTime.Now);
                    var activity2 = new TestDatabase.Activity(
                        workflow.WorkflowName,
                        $"Activity-{i}-2",
                        activity1.ActivityName,
                        TestDatabase.ActivityState.Started);
                    var task21 = new TestDatabase.ActivityTask(
                        workflow.WorkflowName,
                        activity1.ActivityName,
                        $"Task-{i}-22",
                        TestDatabase.TaskState.Started,
                        DateTime.Now,
                        null);

                    db.WorkflowTable.AppendRecord(workflow);
                    workflow = workflow with
                    {
                        State = TestDatabase.WorkflowState.Started
                    };
                    db.WorkflowTable.UpdateRecord(workflow, workflow);
                    using (var tx = db.Database.CreateTransaction())
                    {
                        db.ActivityTable.AppendRecord(activity1, tx);
                        db.TaskTable.AppendRecord(task11, tx);
                        tx.Complete();
                    }
                    using (var tx = db.Database.CreateTransaction())
                    {
                        task11 = task11 with { State = TestDatabase.TaskState.Completed };
                        db.TaskTable.UpdateRecord(task11, task11, tx);
                        db.TaskTable.AppendRecord(task12, tx);
                        tx.Complete();
                    }
                    using (var tx = db.Database.CreateTransaction())
                    {
                        task12 = task12 with { State = TestDatabase.TaskState.Completed };
                        activity1 = activity1 with { State = TestDatabase.ActivityState.Completed };
                        db.TaskTable.UpdateRecord(task12, task12, tx);
                        db.ActivityTable.UpdateRecord(activity1, activity1, tx);
                        db.ActivityTable.AppendRecord(activity2, tx);
                        db.TaskTable.AppendRecord(task21, tx);
                        tx.Complete();
                    }
                    using (var tx = db.Database.CreateTransaction())
                    {
                        task21 = task21 with { State = TestDatabase.TaskState.Completed };
                        activity2 = activity2 with { State = TestDatabase.ActivityState.Completed };
                        workflow = workflow with { State = TestDatabase.WorkflowState.Completed };
                        db.TaskTable.UpdateRecord(task21, task21, tx);
                        db.ActivityTable.UpdateRecord(activity2, activity2, tx);
                        db.WorkflowTable.UpdateRecord(workflow, workflow, tx);
                        tx.Complete();
                    }
                    var incompleteTasks = db.TaskTable.Query()
                        .Where(pf => pf.NotEqual(w => w.State, TestDatabase.TaskState.Completed))
                        .ToImmutableArray();

                    Assert.Empty(incompleteTasks);
                    await db.Database.AwaitLifeCycleManagement(5);
                    if (i % reloadCycleCount == 0)
                    {
                        await ((IAsyncDisposable)db).DisposeAsync();
                        db = await TestDatabase.CreateAsync(testId);
                    }
                }

                CheckDb(db, cycleCount);

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine(stats);
                await ((IAsyncDisposable)db).DisposeAsync();
            }
            //  Check final state after reloading
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                CheckDb(db, cycleCount);

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine(
                    $"Cycle count = {cycleCount} ({stopwatch.Elapsed}):  " +
                    $"Block Count ({stats.GlobalStatistics.Persisted?.BlockCount}), " +
                    $"Persisted Size ({stats.GlobalStatistics.Persisted?.Size}), " +
                    $"Persisted Record per block ({stats.GlobalStatistics?.Persisted?.RecordPerBlock}), " +
                    $"Max Generation Table ({stats.MaxTableGeneration})");
            }
        }

        private void CheckDb(TestDatabase db, long cycleCount)
        {
            var completedWorkflows = db.WorkflowTable.Query()
                .Where(pf => pf.Equal(w => w.State, TestDatabase.WorkflowState.Completed))
                .Select(w => w.WorkflowName)
                .OrderBy(n => n)
                .ToImmutableArray();
            var completedWorkflowsDistinct = completedWorkflows
                .Distinct()
                .OrderBy(n => n)
                .ToImmutableArray();

            Assert.Equal(cycleCount, completedWorkflowsDistinct.Length);
            Assert.Equal(cycleCount, completedWorkflows.Length);

            var completedWorkflowCount = db.WorkflowTable.Query()
                .Where(pf => pf.Equal(w => w.State, TestDatabase.WorkflowState.Completed))
                .Count();
            var incompleteWorkflows = db.WorkflowTable.Query()
                .Where(pf => pf.NotEqual(w => w.State, TestDatabase.WorkflowState.Completed))
                .ToImmutableArray();

            Assert.Equal(cycleCount, completedWorkflowCount);
            Assert.Empty(incompleteWorkflows);

            var completedActivityCount = db.ActivityTable.Query()
                .Where(pf => pf.Equal(w => w.State, TestDatabase.ActivityState.Completed))
                .Count();
            var incompleteActivities = db.ActivityTable.Query()
                .Where(pf => pf.NotEqual(w => w.State, TestDatabase.ActivityState.Completed))
                .ToImmutableArray();

            Assert.Equal(2 * cycleCount, completedActivityCount);
            Assert.Empty(incompleteActivities);

            var completedTaskCount = db.TaskTable.Query()
                .Where(pf => pf.Equal(w => w.State, TestDatabase.TaskState.Completed))
                .Count();
            var incompleteTasks = db.TaskTable.Query()
                .Where(pf => pf.NotEqual(w => w.State, TestDatabase.TaskState.Completed))
                .ToImmutableArray();

            Assert.Equal(3 * cycleCount, completedTaskCount);
            Assert.Empty(incompleteTasks);
        }
    }
}