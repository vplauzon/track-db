using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.LogTest.Volume
{
    public class AppendBulkTest
    {
        [Fact]
        public async Task Test00010()
        {
            await RunPerformanceTestAsync(10);
        }

        [Fact]
        public async Task Test00050()
        {
            await RunPerformanceTestAsync(50);
        }

        [Fact]
        public async Task Test00250()
        {
            await RunPerformanceTestAsync(250);
        }

        [Fact]
        public async Task Test01000()
        {
            await RunPerformanceTestAsync(1000);
        }

        [Fact]
        public async Task Test010000()
        {
            await RunPerformanceTestAsync(10000);
        }

        [Fact]
        public async Task Test0100000()
        {
            await RunPerformanceTestAsync(100000);
        }

        [Fact]
        public async Task Test01000000()
        {
            await RunPerformanceTestAsync(1000000);
        }

        private async Task RunPerformanceTestAsync(int recordCount)
        {
            var stopwatch = new Stopwatch();
            var testId = $"{GetType().Name}-{recordCount}-{Guid.NewGuid()}";

            stopwatch.Start();
            await using (var db = await TestDatabase.CreateAsync(testId))
            {
                var workflows = Enumerable.Range(0, recordCount)
                    .Select(i => new TestDatabase.Workflow(
                        $"Workflow-{i}",
                        i,
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