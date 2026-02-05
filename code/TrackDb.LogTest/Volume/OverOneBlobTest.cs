using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest.Volume
{
    public class OverOneBlobTest : TestBase
    {
        /// <summary>
        /// This tests does over 50K append blob operations and is hence extremelly long.
        /// That is, multiple hours long.
        /// For this reason, it is disabled by default.
        /// </summary>
        /// <returns></returns>
        //[Fact]
        public async Task OverrunBlob()
        {
            const int CYCLE_COUNT = 50005;

            var stopwatch = new Stopwatch();
            var testId = GetTestId();

            stopwatch.Start();
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                for (int i = 0; i != CYCLE_COUNT; ++i)
                {
                    var workflow = new TestDatabase.Workflow(
                        $"Workflow-{i}",
                        i,
                        false,
                        TestDatabase.WorkflowState.Pending,
                        DateTime.Now);

                    using (var tx = db.Database.CreateTransaction())
                    {
                        db.WorkflowTable.AppendRecord(workflow, tx);

                        //  Force every record to commit a block
                        await tx.CompleteAsync();
                    }
                }

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine(stats);
            }
            //  Check final state after reloading
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                var workflowCounts = db.WorkflowTable.Query()
                    .Count();

                Assert.Equal(CYCLE_COUNT, workflowCounts);

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine(stats);
            }
        }
    }
}