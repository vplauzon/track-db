using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.PerfTest
{
    public class BulkInsertTest
    {
        [Fact]
        public async Task Test10by1()
        {
            await RunPerformanceTestAsync(10, 1);
        }

        [Fact]
        public async Task Test10by10()
        {
            await RunPerformanceTestAsync(10, 10);
        }

        [Fact]
        public async Task Test100by10()
        {
            await RunPerformanceTestAsync(100, 10);
        }

        [Fact]
        public async Task Test1000by10()
        {
            await RunPerformanceTestAsync(1000, 10);
        }

        [Fact]
        public async Task Test10000by1()
        {
            await RunPerformanceTestAsync(10000, 1);
        }

        [Fact]
        public async Task Test10000by10()
        {
            await RunPerformanceTestAsync(10000, 10);
        }

        [Fact]
        public async Task Test100000by1()
        {
            await RunPerformanceTestAsync(100000, 1);
        }

        [Fact]
        public async Task Test100000by10()
        {
            await RunPerformanceTestAsync(100000, 10);
        }

        [Fact]
        public async Task Test100000by100()
        {
            await RunPerformanceTestAsync(100000, 100);
        }

        [Fact]
        public async Task Test1000000by1()
        {
            await RunPerformanceTestAsync(1000000, 1);
        }

        protected async Task RunPerformanceTestAsync(int cycleCount, int batchCount)
        {
            var random = new Random();
            var batchSize = cycleCount / batchCount;

            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                for (int b = 0; b != batchCount; ++b)
                {
                    using (var tx = db.Database.CreateTransaction())
                    {
                        var employees = Enumerable.Range(b * batchSize, batchSize)
                            .Select(j => new VolumeTestDatabase.Employee(
                                $"Employee-{j}",
                                $"EmployeeName-{j}"));
                        var requests = employees
                            .Select(e => Enumerable.Range(0, 2)
                            .Select(j => new VolumeTestDatabase.Request(
                                e.EmployeeId,
                                $"Request-{e.Name}-{j}",
                                VolumeTestDatabase.RequestStatus.Initiated)))
                            .SelectMany(x => x);
                        var documents = requests
                            .Select(r => Enumerable.Range(0, 2)
                            .Select(j => new VolumeTestDatabase.Document(
                                r.RequestCode,
                                $"Doc - {random.Next(10000)}")))
                            .SelectMany(x => x);

                        db.EmployeeTable.AppendRecords(employees, tx);
                        db.RequestTable.AppendRecords(requests, tx);
                        db.DocumentTable.AppendRecords(documents, tx);

                        tx.Complete();
                    }
                    //Console.WriteLine(i);
                    await db.Database.AwaitLifeCycleManagement(5);
                }

                Assert.Equal(db.EmployeeTable.Query().Count(), cycleCount);
                Assert.Equal(db.RequestTable.Query().Count(), 2 * cycleCount);
                Assert.Equal(db.DocumentTable.Query().Count(), 4 * cycleCount);

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine($"{cycleCount} by {batchCount}:  {stats}");
            }
        }
    }
}