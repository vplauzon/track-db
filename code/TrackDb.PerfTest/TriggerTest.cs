using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static TrackDb.PerfTest.VolumeTestDatabase;

namespace TrackDb.PerfTest
{
    public class TriggerTest
    {
        [Fact]
        public async Task Test00010By1()
        {
            await RunPerformanceTestAsync(10, 1);
        }

        [Fact]
        public async Task Test00100By1()
        {
            await RunPerformanceTestAsync(100, 1);
        }

        [Fact]
        public async Task Test01000By1()
        {
            await RunPerformanceTestAsync(1000, 1);
        }

        [Fact]
        public async Task Test10000By1()
        {
            await RunPerformanceTestAsync(10000, 1);
        }

        private async Task RunPerformanceTestAsync(int recordCount, int batchSize)
        {
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                SetupData(db, recordCount);
                ValidateSummary(db);

                await db.Database.AwaitLifeCycleManagement(1);
                ValidateSummary(db);
            }
        }

        private void ValidateSummary(VolumeTestDatabase db)
        {
            using (var tx = db.CreateTransaction())
            {
                var onlineSummary = db.TriggeringOrderTable.Query(tx)
                    .GroupBy(m => m.OrderStatus)
                    .Select(g => new OrderSummary(g.Key, g.Count()))
                    .ToDictionary(s => s.OrderStatus, s => s.OrderCount);
                var materializedSummary = db.OrderSummaryTable.Query(tx)
                    .GroupBy(m => m.OrderStatus)
                    .Select(g => new OrderSummary(g.Key, g.Sum(s => s.OrderCount)))
                    .ToDictionary(s => s.OrderStatus, s => s.OrderCount);

                Assert.Equal(onlineSummary.Count(), materializedSummary.Count());
            }
        }

        private static void SetupData(VolumeTestDatabase db, int recordCount)
        {
            var orders = Enumerable.Range(0, recordCount)
                .Select(j => new VolumeTestDatabase.TriggeringOrder(
                    $"Order-{j}",
                    VolumeTestDatabase.OrderStatus.Initiated));

            db.TriggeringOrderTable.AppendRecords(orders);
        }
    }
}