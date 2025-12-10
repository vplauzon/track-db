using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.UnitTest.VolumeTests
{
    public class InsertThenDeleteAllTest
    {
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

        private async Task InsertThenDeleteAllAsync(bool doKeepOne)
        {
            const int N = 10000;

            var random = new Random();

            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                using (var tx1 = db.Database.CreateTransaction())
                {
                    var employees = Enumerable.Range(0, N)
                        .Select(j => new VolumeTestDatabase.Employee(
                            $"Employee-{j}",
                            $"EmployeeName-{j}"));

                    db.EmployeeTable.AppendRecords(employees, tx1);

                    tx1.Complete();
                }
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
    }
}