using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.PerfTest
{
    public class InsertThenDeleteAllTest
    {
        private const int BULK_SIZE = 10000;

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
                foreach (var employeeId in shuffledEmployeeIds)
                {
                    db.EmployeeTable.Query()
                        .Where(pf => pf.Equal(e => e.EmployeeId, employeeId))
                        .Delete();

                    await db.Database.AwaitLifeCycleManagement(4);
                }
                Assert.Equal(
                    0,
                    db.EmployeeTable.Query().Count());
                Assert.Equal(
                    0,
                    db.EmployeeTable.Query().TableQuery.WithInMemoryOnly().Count());
            }
        }

        private async Task InsertThenDeleteAllAsync(bool doKeepOne)
        {
            var random = new Random();

            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                InsertBulk(db);
                await db.Database.AwaitLifeCycleManagement(1);

                var tableMap = db.Database.GetDatabaseStateSnapshot().TableMap;
                var metaEmployee = tableMap[db.EmployeeTable.Schema.TableName].MetaDataTableName;

                Assert.NotNull(metaEmployee);

                var metaMetaEmployee = tableMap[metaEmployee].MetaDataTableName;

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
                await db.Database.AwaitLifeCycleManagement(1);

                Assert.Equal(
                    doKeepOne ? 1 : 0,
                    db.EmployeeTable.Query().Count());
                Assert.Equal(
                    doKeepOne ? 1 : 0,
                    db.EmployeeTable.Query().TableQuery.WithInMemoryOnly().Count());
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