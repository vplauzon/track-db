using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest
{
    public class AppendTest
    {
        [Fact]
        public async Task OneRecordAsync()
        {
            var testId = Guid.NewGuid();
            var record = new TestDatabase.Workflow("My Workflow", 42, DateTime.Now);

            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                db.WorkflowTable.AppendRecord(record);
            }
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                var foundRecord = db.WorkflowTable.Query().FirstOrDefault();

                Assert.NotNull(foundRecord);
                Assert.Equal(record, foundRecord);
            }
        }

        [Fact]
        public async Task MultipleRecordsAsync()
        {
            var testId = Guid.NewGuid();
            var workflow = new TestDatabase.Workflow("My Workflow", 42, DateTime.Now);
            var activity1 = new TestDatabase.Activity(
                workflow.WorkflowName,
                "Activity-1",
                null,
                TestDatabase.ActivityState.Started);
            var activity2 = new TestDatabase.Activity(
                workflow.WorkflowName,
                "Activity-2",
                activity1.ActivityName,
                TestDatabase.ActivityState.Started);
            var task1 = new TestDatabase.ActivityTask(
                workflow.WorkflowName,
                activity2.ActivityName,
                "Task 1",
                DateTime.Now,
                null);
            var task2 = new TestDatabase.ActivityTask(
                workflow.WorkflowName,
                activity2.ActivityName,
                "Task 1",
                task1.StartTime,
                DateTime.Now);

            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                db.WorkflowTable.AppendRecord(workflow);
                db.ActivityTable.AppendRecord(activity1);
                db.ActivityTable.AppendRecord(activity2);
                db.TaskTable.AppendRecord(task1);
                db.TaskTable.AppendRecord(task2);
            }
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                var foundWorkflows = db.WorkflowTable.Query().ToImmutableArray();
                var foundActivities = db.ActivityTable.Query()
                    .OrderBy(a => a.ActivityName)
                    .ToImmutableArray();
                var foundTasks = db.TaskTable.Query()
                    .OrderBy(t => t.TaskName)
                    .ToImmutableArray();

                Assert.Single(foundWorkflows);
                Assert.Equal(workflow, foundWorkflows[0]);

                Assert.Equal(2, foundActivities.Length);
                Assert.Equal(activity1, foundActivities[0]);
                Assert.Equal(activity2, foundActivities[1]);

                Assert.Equal(2, foundTasks.Length);
                Assert.Equal(task1, foundTasks[0]);
                Assert.Equal(task2, foundTasks[1]);
            }
        }
    }
}