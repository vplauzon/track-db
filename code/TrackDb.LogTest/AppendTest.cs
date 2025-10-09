using System;
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
    }
}