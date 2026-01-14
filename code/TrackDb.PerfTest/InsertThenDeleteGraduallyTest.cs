using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.PerfTest
{
    public class InsertThenDeleteRandomTest
    {
        [Fact]
        public async Task Test000010()
        {
            await RunPerformanceTestAsync(10);
        }

        [Fact]
        public async Task Test000100()
        {
            await RunPerformanceTestAsync(100);
        }

        [Fact]
        public async Task Test001000()
        {
            await RunPerformanceTestAsync(1000);
        }

        [Fact]
        public async Task Test002000()
        {
            await RunPerformanceTestAsync(2000);
        }

        [Fact]
        public async Task Test010000()
        {
            await RunPerformanceTestAsync(10000);
        }

        private async Task RunPerformanceTestAsync(int bulkSize)
        {
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                InsertBulk(db, bulkSize);

                var shuffledEmployeeIds = db.EmployeeTable.Query()
                    .Select(e => e.EmployeeId)
                    .Shuffle()
                    .ToImmutableArray();

                //  Delete out-of-order, one at the time
                for (var i = 0; i != shuffledEmployeeIds.Length; ++i)
                {
                    var employeeId = shuffledEmployeeIds[i];

                    db.EmployeeTable.Query()
                        .Where(pf => pf.Equal(e => e.EmployeeId, employeeId))
                        .Delete();

                    await db.Database.AwaitLifeCycleManagement(4);
                    if (i % 100 == 0)
                    {
                        Assert.Equal(bulkSize - i - 1, db.EmployeeTable.Query().Count());
                    }
                }
                Assert.Equal(0, db.EmployeeTable.Query().Count());
            }
        }

        private static void InsertBulk(VolumeTestDatabase db, int bulkSize)
        {
            using (var tx = db.Database.CreateTransaction())
            {
                var employees = Enumerable.Range(0, bulkSize)
                    .Select(j => new VolumeTestDatabase.Employee(
                        $"Employee-{j}",
                        $"EmployeeName-{j}"));

                db.EmployeeTable.AppendRecords(employees, tx);

                tx.Complete();
            }
        }
    }
}