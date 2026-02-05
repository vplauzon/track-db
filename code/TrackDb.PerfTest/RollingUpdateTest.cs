using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.PerfTest
{
    public class RollingUpdateTest
    {
        public RollingUpdateTest()
        {
        }

        [Fact]
        public async Task Test000010With003()
        {
            await RunPerformanceTestAsync(10, 3);
        }

        [Fact]
        public async Task Test000100With003()
        {
            await RunPerformanceTestAsync(100, 3);
        }

        [Fact]
        public async Task Test001000With003()
        {
            await RunPerformanceTestAsync(1000, 3);
        }

        [Fact]
        public async Task Test001000With010()
        {
            await RunPerformanceTestAsync(1000, 10);
        }

        [Fact]
        public async Task Test002000With010()
        {
            await RunPerformanceTestAsync(2000, 10);
        }

        [Fact]
        public async Task Test002000With100()
        {
            await RunPerformanceTestAsync(2000, 100);
        }

        [Fact]
        public async Task Test010000With010()
        {
            await RunPerformanceTestAsync(10000, 10);
        }

        private async Task RunPerformanceTestAsync(int entityCount, int subEntityCount)
        {
            var random = new Random();

            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                //  Setup employees & requests
                var employees = Enumerable.Range(0, entityCount)
                    .Select(i => new VolumeTestDatabase.Employee(
                        $"Employee-{i}",
                        $"EmployeeName-{i}"));
                var requests = Enumerable.Range(0, entityCount)
                    .Select(i => new VolumeTestDatabase.Request(
                        $"Employee-{i}",
                        $"Request-{i}",
                        VolumeTestDatabase.RequestStatus.Initiated));

                db.EmployeeTable.AppendRecords(employees);
                db.RequestTable.AppendRecords(requests);
                //  Transition from Initiated to Treating & create documents
                for (int i = 0; i != entityCount; ++i)
                {
                    using (var tx = db.CreateTransaction())
                    {
                        var request = db.RequestTable.Query(tx)
                            .Where(pf => pf.Equal(r => r.EmployeeId, $"Employee-{i}"))
                            .First();

                        db.RequestTable.UpdateRecord(
                            request,
                            request with
                            {
                                RequestStatus = VolumeTestDatabase.RequestStatus.Treating
                            },
                            tx);

                        tx.Complete();
                    }
                    await db.AwaitLifeCycleManagementAsync(2);
                }
                //  Delete everything
                for (int i = 0; i != entityCount; ++i)
                {
                    using (var tx = db.CreateTransaction())
                    {
                        var employeeDeleteCount = db.EmployeeTable.Query(tx)
                            .Where(pf => pf.Equal(e => e.EmployeeId, $"Employee-{i}"))
                            .Delete();
                        var requestDeleteCount = db.RequestTable.Query(tx)
                            .Where(pf => pf.Equal(r => r.EmployeeId, $"Employee-{i}"))
                            .Delete();
                        var documentDeleteCount = db.DocumentTable.Query(tx)
                            .Where(pf => pf.Equal(d => d.RequestCode, $"Request-{i}"))
                            .Delete();

                        tx.Complete();
                    }

                    await db.AwaitLifeCycleManagementAsync(2);
                }

                Assert.Equal(0, db.EmployeeTable.Query().Count());
                Assert.Equal(0, db.RequestTable.Query().Count());
                Assert.Equal(0, db.DocumentTable.Query().Count());
            }
        }
    }
}