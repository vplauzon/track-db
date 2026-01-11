using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

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
                Assert.Equal(1, db.OrderSummaryTable.Query().Count());
                Assert.Equal(
                    VolumeTestDatabase.OrderStatus.Initiated,
                    db.OrderSummaryTable.Query().First().OrderStatus);
                Assert.Equal(recordCount, db.OrderSummaryTable.Query().First().OrderCount);

                await db.Database.AwaitLifeCycleManagement(1);
                Assert.Equal(1, db.OrderSummaryTable.Query().Count());
                Assert.Equal(
                    VolumeTestDatabase.OrderStatus.Initiated,
                    db.OrderSummaryTable.Query().First().OrderStatus);
                Assert.Equal(recordCount, db.OrderSummaryTable.Query().First().OrderCount);
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