using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest
{
    public class AppendDeleteTest : TestBase
    {
        [Fact]
        public async Task OneRecord()
        {
            var testId = GetTestId();
            var record1 = new TestDatabase.Workflow(
                "My Workflow",
                42,
                true,
                TestDatabase.WorkflowState.Started,
                DateTime.Now);
            var record2 = record1 with { State = TestDatabase.WorkflowState.Pending };

            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                db.WorkflowTable.AppendRecord(record1);
                db.WorkflowTable.UpdateRecord(record1, record2);
            }
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                var foundRecord = db.WorkflowTable.Query().FirstOrDefault();

                Assert.NotNull(foundRecord);
                Assert.Equal(record2, foundRecord);
            }
        }
    }
}