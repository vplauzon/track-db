using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.UnitTest.VolumeTests
{
    public abstract class InsertUpdateTestBase
    {
        private readonly bool _doUpdate;

        protected InsertUpdateTestBase(bool doUpdate)
        {
            _doUpdate = doUpdate;
        }

        protected async Task RunPerformanceTestAsync(int cycleCount)
        {
            var stopwatch = new Stopwatch();
            var random = new Random();

            stopwatch.Start();
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                for (int i = 0; i != cycleCount; ++i)
                {
                    var employee = new VolumeTestDatabase.Employee(
                        $"Employee-{i}",
                        $"EmployeeName-{i}");
                    var request1 = new VolumeTestDatabase.Request(
                        employee.EmployeeId,
                        $"Request-{2 * i}-1",
                        VolumeTestDatabase.RequestStatus.Initiated);
                    var request2 = new VolumeTestDatabase.Request(
                        employee.EmployeeId,
                        $"Request-{2 * i + 1}-2",
                        VolumeTestDatabase.RequestStatus.Initiated);
                    var document11 = new VolumeTestDatabase.Document(
                        request1.RequestCode,
                        $"Doc1 - {random.Next(100)}");
                    var document12 = new VolumeTestDatabase.Document(
                        request1.RequestCode,
                        $"Doc2 - {random.Next(100)}");
                    var document21 = new VolumeTestDatabase.Document(
                        request2.RequestCode,
                        $"Doc3 - {random.Next(100)}");

                    db.EmployeeTable.AppendRecord(employee);

                    db.RequestTable.AppendRecord(request1);
                    db.RequestTable.AppendRecord(request2);
                    if (_doUpdate)
                    {
                        db.RequestTable.UpdateRecord(
                            request1,
                            request1 with { RequestStatus = VolumeTestDatabase.RequestStatus.Treating });
                        db.RequestTable.UpdateRecord(
                            request2,
                            request2 with { RequestStatus = VolumeTestDatabase.RequestStatus.Treating });
                    }
                    db.DocumentTable.AppendRecord(document11);
                    db.DocumentTable.AppendRecord(document12);
                    db.DocumentTable.AppendRecord(document21);

                    if (_doUpdate)
                    {
                        db.RequestTable.UpdateRecord(
                            request1,
                            request1 with { RequestStatus = VolumeTestDatabase.RequestStatus.Completed });
                        db.RequestTable.UpdateRecord(
                            request2,
                            request2 with { RequestStatus = VolumeTestDatabase.RequestStatus.Completed });
                    }
                    await db.Database.AwaitLifeCycleManagement(5);

                    Console.WriteLine(i);
                }

                Assert.Equal(db.EmployeeTable.Query().Count(), cycleCount);
                Assert.Equal(db.RequestTable.Query().Count(), 2 * cycleCount);
                Assert.Equal(db.DocumentTable.Query().Count(), 3 * cycleCount);

                var stats = db.Database.GetDatabaseStatistics();

                Console.WriteLine($"{cycleCount}:  {stats}");

                ValidateData(db, cycleCount);
            }
        }

        private static void ValidateData(VolumeTestDatabase db, int cycleCount)
        {
            var groups = db.DocumentTable.Query()
                .Select(d => new
                {
                    DocNumber = int.Parse(d.DocumentContent.Split('-')[0].Trim().Substring(3)),
                    RequestCodeParts = d.RequestCode.Split('-')
                })
                .Select(o => new
                {
                    o.DocNumber,
                    RequestNumber = int.Parse(o.RequestCodeParts[2]),
                    EmployeeNumber = int.Parse(o.RequestCodeParts[1]) / 2
                })
                .GroupBy(o => o.EmployeeNumber)
                .ToImmutableDictionary(
                g => g.Key,
                g => g.GroupBy(o => o.RequestNumber).ToImmutableDictionary(
                    g2 => g2.Key,
                    g2 => g2.Select(o => o.DocNumber).Order().ToArray())
                );

            Assert.True(Enumerable.SequenceEqual(
                Enumerable.Range(0, cycleCount),
                groups.Keys.Order()));
            foreach (var employeeNumber in groups.Keys)
            {
                var subGroup = groups[employeeNumber];

                Assert.True(Enumerable.SequenceEqual(
                    Enumerable.Range(1, 2),
                    subGroup.Keys.Order()));

                Assert.True(Enumerable.SequenceEqual(
                    Enumerable.Range(1, 2),
                    subGroup[1]));
                Assert.True(Enumerable.SequenceEqual(
                    Enumerable.Range(3, 1),
                    subGroup[2]));
            }
        }
    }
}