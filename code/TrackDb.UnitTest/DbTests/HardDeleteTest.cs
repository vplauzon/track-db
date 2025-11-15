using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.UnitTest.DbTests
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
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);

                Assert.True(db.PrimitiveTable.Query().Count() == 1);

                db.PrimitiveTable.Query()
                    .Where(pf => pf.Equal(r => r.Integer, 1))
                    .Delete();

                Assert.True(db.PrimitiveTable.Query().Count() == 0);
                using (var tx = db.Database.CreateTransaction())
                {
                    Assert.True(tx.TransactionState.InMemoryDatabase.TransactionTableLogsMap.ContainsKey(
                        db.PrimitiveTable.Schema.TableName));
                }

                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.True(db.PrimitiveTable.Query().Count() == 0);
                using (var tx = db.Database.CreateTransaction())
                {
                    Assert.False(tx.TransactionState.InMemoryDatabase.TransactionTableLogsMap.ContainsKey(
                        db.PrimitiveTable.Schema.TableName));
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Parallel(bool doPushPendingData)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);

                db.PrimitiveTable.AppendRecord(record1);
                db.PrimitiveTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);

                Assert.True(db.PrimitiveTable.Query().Count() == 2);

                var tx1 = db.Database.CreateTransaction();
                var tx2 = db.Database.CreateTransaction();

                //  In tc1, we delete first one, in tc2, we delete both
                db.PrimitiveTable.Query(tx1)
                    .Where(pf => pf.Equal(r => r.Integer, 1))
                    .Delete();
                db.PrimitiveTable.Query(tx2)
                    .Delete();

                tx1.Complete();

                Assert.Equal(1, db.PrimitiveTable.Query().Count());

                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);
                if (doPushPendingData)
                {
                    await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);
                }

                Assert.Equal(1, db.PrimitiveTable.Query().Count());

                tx2.Complete();
                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(0, db.PrimitiveTable.Query().Count());
                using (var tc = db.Database.CreateTransaction())
                {
                    Assert.False(
                        tc.TransactionState.ListBlocks(
                            db.PrimitiveTable.Schema.TableName).Any());
                }
            }
        }
    }
}