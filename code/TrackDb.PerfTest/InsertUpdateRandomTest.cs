using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using static TrackDb.PerfTest.VolumeTestDatabase;

namespace TrackDb.PerfTest
{
    public class InsertUpdateRandomTest
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

        protected async Task RunPerformanceTestAsync(int cycleCount)
        {
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                //  Initial data
                db.EmployeeTable.AppendRecords(Enumerable.Range(0, cycleCount)
                    .Select(i => new VolumeTestDatabase.Employee(
                        $"Employee-{i}",
                        $"EmployeeName-{i}")));
                db.RequestTable.AppendRecords(Enumerable.Range(0, cycleCount)
                    .Select(i => new VolumeTestDatabase.Request(
                        $"Employee-{i}",
                        $"Request-{i}-1",
                        VolumeTestDatabase.RequestStatus.Initiated)));
                db.RequestTable.AppendRecords(Enumerable.Range(0, cycleCount)
                    .Select(i => new VolumeTestDatabase.Request(
                        $"Employee-{i}",
                        $"Request-{i}-2",
                        VolumeTestDatabase.RequestStatus.Initiated)));
                await db.Database.AwaitLifeCycleManagement(5);

                var employeeIds = db.EmployeeTable.Query()
                    .Select(e => e.EmployeeId)
                    .ToImmutableArray()
                    .Shuffle()
                    .ToImmutableArray();
                var i = 0;

                foreach (var employeeId in employeeIds)
                {
                    using (var tx = db.CreateTransaction())
                    {
                        var employees = db.EmployeeTable.Query(tx)
                            .Where(pf => pf.Equal(e => e.EmployeeId, employeeId))
                            .ToImmutableArray();
                        var requests = db.RequestTable.Query(tx)
                            .Where(pf => pf.Equal(r => r.EmployeeId, employeeId))
                            .ToImmutableArray();

                        Assert.Single(employees);
                        Assert.Equal(2, requests.Length);

                        var employee = employees[0];

                        db.EmployeeTable.UpdateRecord(
                            employee,
                            employee with { Name = $"E-{employee.Name}" },
                            tx);

                        foreach (var request in requests)
                        {
                            db.RequestTable.UpdateRecord(
                                request,
                                request with
                                {
                                    RequestStatus = VolumeTestDatabase.RequestStatus.Completed
                                },
                                tx);
                        }
                        ++i;

                        tx.Complete();
                    }
                    await db.Database.AwaitLifeCycleManagement(5);
                    if (i % 5 == 0)
                    {
                        Assert.Equal(cycleCount, db.EmployeeTable.Query().Count());
                        Assert.Equal(2 * cycleCount, db.RequestTable.Query().Count());
                    }
                }

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine($"{cycleCount}:  {stats}");

                ValidateData(db, cycleCount);
            }
        }

        private static void ValidateData(VolumeTestDatabase db, int cycleCount)
        {
            Assert.Equal(cycleCount, db.EmployeeTable.Query().Count());
            Assert.Equal(2 * cycleCount, db.RequestTable.Query().Count());

            Assert.Equal(
                0,
                db.RequestTable.Query()
                .Where(pf => pf.NotEqual(
                    r => r.RequestStatus,
                    VolumeTestDatabase.RequestStatus.Completed))
                .Count());
        }
    }
}