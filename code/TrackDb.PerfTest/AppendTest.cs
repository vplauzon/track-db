using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.PerfTest
{
    public class AppendTest
    {
        [Fact]
        public async Task AppendAndSegment()
        {
            const int RECORD_COUNT = 5000;

            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                var random = new Random();
                var records = Enumerable.Range(0, RECORD_COUNT)
                    .Select(i => new VolumeTestDatabase.Employee(
                        $"{i}",
                        $"Bob{i} Smith{i + 50}"))
                    .ToImmutableArray();

                //  Append in one transaction
                db.EmployeeTable.AppendRecords(records);
                await db.Database.ForceDataManagementAsync(
                    DataManagementActivity.PersistAllNonMetaData
                    | DataManagementActivity.PersistAllMetaDataFirstLevel);

                var min = db.EmployeeTable.Query()
                    .Min(e => int.Parse(e.EmployeeId));
                var max = db.EmployeeTable.Query()
                    .Max(e => int.Parse(e.EmployeeId));

                Assert.Equal(0, min);
                Assert.Equal(RECORD_COUNT - 1, max);

                Console.WriteLine($"Stats:  {db.Database.GetDatabaseStatistics()}");
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AppendBunch(bool doPushPendingData)
        {
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                var random = new Random();
                var records = Enumerable.Range(0, 5000)
                    .Select(i => new VolumeTestDatabase.Employee(
                        $"Emp-{i}",
                        $"Bob{i} Smith{i + 50}"))
                    .ToImmutableArray();

                //  Append in one transaction
                db.EmployeeTable.AppendRecords(records);
                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var resultsAll = db.EmployeeTable.Query()
                    .ToImmutableList();

                Assert.Equal(records.Length, resultsAll.Count);
                foreach (var r in records)
                {
                    Assert.Contains(r, resultsAll);
                }

                Console.WriteLine($"Stats:  {db.Database.GetDatabaseStatistics()}");
            }
        }

        [Fact]
        public async Task AppendAndQuery()
        {
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                const int TOTAL = 10000;
                const int BATCH = 200;

                var random = new Random();
                var records = Enumerable.Range(0, TOTAL)
                    .Select(i => new VolumeTestDatabase.Employee(
                        $"Emp-{random.Next(20000)}",
                        $"Bob-{random.Next(100)}"))
                    .ToImmutableArray();

                for (var i = 0; i != TOTAL / BATCH; ++i)
                {
                    db.EmployeeTable.AppendRecords(records.Skip(i * BATCH).Take(BATCH));
                    await db.Database.ForceDataManagementAsync(
                        DataManagementActivity.PersistAllNonMetaData);
                }

                var resultsAll = db.EmployeeTable.Query()
                    .ToImmutableList();

                Assert.Equal(records.Length, resultsAll.Count);
                Assert.Equal(
                    records.Sum(r => long.Parse(r.EmployeeId.Split('-')[1])),
                    resultsAll.Sum(r => long.Parse(r.EmployeeId.Split('-')[1])));

                Console.WriteLine($"Stats:  {db.Database.GetDatabaseStatistics()}");
            }
        }
    }
}