using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest.Volume
{
    public class AppendBulkTest : TestBase
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
        public async Task Test010000()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 10000);
        }

        [Fact]
        public async Task Test0100000()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 100000);
        }

        [Fact]
        public async Task Test01000000()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 1000000);
        }

        [Fact]
        public async Task Test02000000()
        {
            var testId = GetTestId();

            await RunPerformanceTestAsync(testId, 2000000);
        }

        private async Task RunPerformanceTestAsync(string testId, int recordCount)
        {
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                var workflows = Enumerable.Range(0, recordCount)
                    .Select(i => new TestDatabase.Workflow(
                        $"Workflow-{i}",
                        i,
                        true,
                        TestDatabase.WorkflowState.Pending,
                        DateTime.Now));

                db.WorkflowTable.AppendRecords(workflows);

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine(stats);
            }
            //  Check final state after reloading
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                var workflowCounts = db.WorkflowTable.Query()
                    .Count();

                Assert.Equal(recordCount, workflowCounts);

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine(stats);
            }
        }
    }
}