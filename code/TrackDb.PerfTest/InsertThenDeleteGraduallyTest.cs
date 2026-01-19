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
        public async Task TestGen0()
        {
            await RunPerformanceTestAsync(0);
        }

        [Fact]
        public async Task TestGen1()
        {
            await RunPerformanceTestAsync(1);
        }

        [Fact]
        public async Task TestGen2()
        {
            await RunPerformanceTestAsync(2);
        }

        [Fact]
        public async Task TestGen3()
        {
            await RunPerformanceTestAsync(3);
        }

        private async Task RunPerformanceTestAsync(int generation)
        {
            await using (var db = await VolumeTestDatabase.CreateAsync())
            {
                await InsertBulkAsync(db, generation);

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
                    //if (i % 100 == 0)
                    {
                        Assert.Equal(
                            shuffledEmployeeIds.Length - i - 1,
                            db.EmployeeTable.Query().Count());
                    }
                }
                Assert.Equal(0, db.EmployeeTable.Query().Count());
            }
        }

        private static async Task InsertBulkAsync(VolumeTestDatabase db, int generation)
        {
            const int GEN0_BATCH_SIZE = 50;
            const int GENERAL_BATCH_SIZE = 500;

            int employeeId = 1;
            bool hasReachedGeneration = false;

            while (!hasReachedGeneration)
            {
                using (var tx = db.Database.CreateTransaction())
                {
                    var bulkSize = generation == 0 ? GEN0_BATCH_SIZE : GENERAL_BATCH_SIZE;
                    var employees = Enumerable.Range(employeeId, bulkSize)
                        .Select(j => new VolumeTestDatabase.Employee(
                            $"Employee-{j}",
                            $"EmployeeName-{j}"));

                    db.EmployeeTable.AppendRecords(employees, tx);
                    employeeId += bulkSize;

                    tx.Complete();
                }
                await db.Database.AwaitLifeCycleManagement(2);

                //  Evaluate hasReachedGeneration

                var table = GetTableGeneration(db.Database, db.EmployeeTable, generation);

                hasReachedGeneration = table.Query().Take(1).Any();
            }
        }

        private static Table GetTableGeneration(Database database, Table table, int generation)
        {
            for (var i = 0; i != generation; ++i)
            {
                table = database.GetMetaDataTable(table.Schema.TableName);
            }

            return table;
        }
    }
}