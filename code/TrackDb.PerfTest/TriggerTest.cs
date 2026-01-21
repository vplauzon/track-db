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
        public async Task Test00010By01()
        {
            await RunPerformanceTestAsync(10, 1);
        }

        [Fact]
        public async Task Test00100By01()
        {
            await RunPerformanceTestAsync(100, 1);
        }

        [Fact]
        public async Task Test01000By01()
        {
            await RunPerformanceTestAsync(1000, 1);
        }

        [Fact]
        public async Task Test01000By10()
        {
            for (var i = 0; i != 10; ++i)
            {
                await RunPerformanceTestAsync(1000, 10);
            }
        }

        [Fact]
        public async Task Test10000By10()
        {
            await RunPerformanceTestAsync(10000, 10);
        }

        private async Task RunPerformanceTestAsync(int recordCount, int batchSize)
        {
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                SetupData(db, recordCount);
                await TransitionAsync(
                    db,
                    batchSize,
                    OrderStatus.Initiated,
                    OrderStatus.Processing);
                await TransitionAsync(
                    db,
                    batchSize,
                    OrderStatus.Processing,
                    OrderStatus.Completed);
            }
        }

        private async Task TransitionAsync(
            VolumeTestDatabase db,
            int batchSize,
            OrderStatus fromStatus,
            OrderStatus toStatus)
        {
            while (true)
            {
                using (var tx = db.CreateTransaction())
                {
                    var orders = db.TriggeringOrderTable.Query(tx)
                        .Where(pf => pf.Equal(o => o.OrderStatus, fromStatus))
                        .Take(batchSize)
                        .ToImmutableArray();

                    if (orders.Length == 0)
                    {
                        return;
                    }
                    else
                    {
                        db.TriggeringOrderTable.Query(tx)
                            .Where(pf => pf.In(o => o.OrderId, orders.Select(o => o.OrderId)))
                            .Delete();
                        db.TriggeringOrderTable.AppendRecords(
                            orders
                            .Select(o => o with { OrderStatus = toStatus }),
                            tx);

                        tx.Complete();
                    }
                }
                await db.Database.AwaitLifeCycleManagement(2);
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
                    .Where(s => s.OrderCount != 0)
                    .ToDictionary(s => s.OrderStatus, s => s.OrderCount);

                Assert.Equal(onlineSummary.Count(), materializedSummary.Count());
                foreach (var status in onlineSummary.Keys)
                {
                    Assert.Equal(onlineSummary[status], materializedSummary[status]);
                }
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