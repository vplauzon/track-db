using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest
{
    public class PerformanceTest
    {
        [Fact]
        public async Task Test50()
        {
            await RunPerformanceTestAsync(50);
        }

        private async Task RunPerformanceTestAsync(long cycleCount)
        {
            var testId = Guid.NewGuid();

            await using (var db = await TestDatabase.CreateAsync(testId))
            {
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
                }
            }
            //  Check final state after reloading
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                var completedWorkflowCount = db.WorkflowTable.Query()
                    .Where(pf => pf.Equal(w => w.State, TestDatabase.WorkflowState.Completed))
                    .Count();
                var incompleteWorkflowCount = db.WorkflowTable.Query()
                    .Where(pf => pf.NotEqual(w => w.State, TestDatabase.WorkflowState.Completed))
                    .Count();

                Assert.Equal(cycleCount, completedWorkflowCount);
                Assert.Equal(0, incompleteWorkflowCount);

                var completedActivityCount = db.ActivityTable.Query()
                    .Where(pf => pf.Equal(w => w.State, TestDatabase.ActivityState.Completed))
                    .Count();
                var incompleteActivityCount = db.ActivityTable.Query()
                    .Where(pf => pf.NotEqual(w => w.State, TestDatabase.ActivityState.Completed))
                    .Count();

                Assert.Equal(2 * cycleCount, completedActivityCount);
                Assert.Equal(0, incompleteActivityCount);

                var completedTaskCount = db.TaskTable.Query()
                    .Where(pf => pf.Equal(w => w.State, TestDatabase.TaskState.Completed))
                    .Count();
                var incompleteTaskCount = db.TaskTable.Query()
                    .Where(pf => pf.NotEqual(w => w.State, TestDatabase.TaskState.Completed))
                    .Count();

                Assert.Equal(3 * cycleCount, completedTaskCount);
                Assert.Equal(0, incompleteTaskCount);
            }
        }
    }
}