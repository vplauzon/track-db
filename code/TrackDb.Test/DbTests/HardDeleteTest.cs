using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class HardDeleteTest
    {
        [Fact]
        public async Task OneRecord()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record = new TestDatabase.Primitives(1);

                db.PrimitiveTable.AppendRecord(record);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllUserData);

                Assert.True(db.PrimitiveTable.Query().Count() == 1);

                db.PrimitiveTable.Query()
                    .Where(pf => pf.Equal(r => r.Integer, 1))
                    .Delete();

                Assert.True(db.PrimitiveTable.Query().Count() == 0);
                using (var tc = db.Database.CreateTransaction())
                {
                    Assert.True(
                        tc.TransactionState.InMemoryDatabase.TableTransactionLogsMap.Any());
                }

                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.True(db.PrimitiveTable.Query().Count() == 0);
                using (var tc = db.Database.CreateTransaction())
                {
                    Assert.False(tc.TransactionState.InMemoryDatabase.TableTransactionLogsMap.ContainsKey(
                        db.PrimitiveTable.Schema.TableName));
                }
            }
        }

        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task Parallel(bool doPushPendingData)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);

                db.PrimitiveTable.AppendRecord(record1);
                db.PrimitiveTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllUserData);

                Assert.True(db.PrimitiveTable.Query().Count() == 2);

                var tc1 = db.Database.CreateTransaction();
                var tc2 = db.Database.CreateTransaction();

                //  In tc1, we delete first one, in tc2, we delete both
                db.PrimitiveTable.Query(tc1)
                    .Where(pf => pf.Equal(r => r.Integer, 1))
                    .Delete();
                db.PrimitiveTable.Query(tc2)
                    .Delete();

                tc1.Complete();

                Assert.Equal(1, db.PrimitiveTable.Query().Count());

                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);
                if (doPushPendingData)
                {
                    await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllUserData);
                }

                Assert.Equal(1, db.PrimitiveTable.Query().Count());

                tc2.Complete();
                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(0, db.PrimitiveTable.Query().Count());
                using (var tc = db.Database.CreateTransaction())
                {
                    Assert.False(
                        tc.TransactionState.ListTransactionLogBlocks(
                            db.PrimitiveTable.Schema.TableName).Any());
                }
            }
        }
    }
}