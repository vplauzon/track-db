using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.PerfTest
{
    public class InsertThenDeleteAllTest
    {
        private const int BULK_SIZE = 20000;

        [Fact]
        public async Task DeleteAll()
        {
            await InsertThenDeleteAllAsync(false);
        }

        [Fact]
        public async Task DeleteAllButOne()
        {
            await InsertThenDeleteAllAsync(true);
        }

        [Fact]
        public async Task RandomDelete()
        {
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                InsertBulk(db);

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

                    await db.AwaitLifeCycleManagementAsync(4);
                }
                Assert.Equal(0, db.EmployeeTable.Query().Count());
                Assert.Equal(0, db.EmployeeTable.Query().TableQuery.WithInMemoryOnly().Count());
            }
        }

        private async Task InsertThenDeleteAllAsync(bool doKeepOne)
        {
            var random = new Random();

            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                InsertBulk(db);
                await db.AwaitLifeCycleManagementAsync(1);

                var tableMap = db.Database.GetDatabaseStateSnapshot().TableMap;
                var metaEmployee = tableMap[db.EmployeeTable.Schema.TableName].MetadataTableName;

                Assert.NotNull(metaEmployee);

                var metaMetaEmployee = tableMap[metaEmployee].MetadataTableName;

                Assert.NotNull(metaMetaEmployee);
                Assert.True(tableMap[metaMetaEmployee].Table.Query().Count() > 0);
                using (var tx2 = db.Database.CreateTransaction())
                {
                    if (doKeepOne)
                    {   //  Delete all but the first one
                        db.EmployeeTable.Query(tx2)
                            .Where(pf => pf.NotEqual(e => e.EmployeeId, "Employee-0"))
                            .Delete();
                    }
                    else
                    {
                        db.EmployeeTable.Query(tx2)
                            .Delete();
                    }

                    tx2.Complete();
                }
                await db.AwaitLifeCycleManagementAsync(1);

                Assert.Equal(
                    doKeepOne ? 1 : 0,
                    db.EmployeeTable.Query().Count());
                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);
                Assert.Equal(
                    doKeepOne ? 1 : 0,
                    db.EmployeeTable.Query().TableQuery.Count());
            }
        }

        private static void InsertBulk(VolumeTestDatabase db)
        {
            using (var tx1 = db.Database.CreateTransaction())
            {
                var employees = Enumerable.Range(0, BULK_SIZE)
                    .Select(j => new VolumeTestDatabase.Employee(
                        $"Employee-{j}",
                        $"EmployeeName-{j}"));

                db.EmployeeTable.AppendRecords(employees, tx1);

                tx1.Complete();
            }
        }
    }
}