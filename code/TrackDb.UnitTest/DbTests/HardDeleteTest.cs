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

                Assert.Equal(1, db.PrimitiveTable.Query().Count());

                db.PrimitiveTable.Query()
                    .Where(pf => pf.Equal(r => r.Integer, 1))
                    .Delete();

                Assert.Equal(0, db.PrimitiveTable.Query().Count());
                Assert.True(db.Database.TombstoneTable.Query().Count() > 0);

                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(0, db.PrimitiveTable.Query().Count());
                Assert.Equal(
                    0,
                    db.Database.TombstoneTable.Query()
                    .Where(pf => pf.Equal(t => t.TableName, db.PrimitiveTable.Schema.TableName))
                    .Count());
            }
        }

        [Fact]
        public async Task TwoRecordsOneDeleted()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);

                db.PrimitiveTable.AppendRecord(record1);
                db.PrimitiveTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);

                Assert.Equal(2, db.PrimitiveTable.Query().Count());

                db.PrimitiveTable.Query()
                    .Where(pf => pf.Equal(r => r.Integer, record1.Integer))
                    .Delete();

                Assert.Equal(1, db.PrimitiveTable.Query().Count());
                Assert.True(db.Database.TombstoneTable.Query().Count() > 0);

                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(1, db.PrimitiveTable.Query().Count());
                Assert.Equal(
                    0,
                    db.Database.TombstoneTable.Query()
                    .Where(pf => pf.Equal(t => t.TableName, db.PrimitiveTable.Schema.TableName))
                    .Count());
            }
        }

        [Fact]
        public async Task ManyRecords()
        {
            const int LOOP_SIZE = 20;

            await using (var db = await TestDatabase.CreateAsync())
            {
                //  Create blocks of two records
                for (var i = 0; i != LOOP_SIZE; ++i)
                {
                    var record1 = new TestDatabase.Primitives(2 * i);
                    var record2 = new TestDatabase.Primitives(2 * i + 1);

                    db.PrimitiveTable.AppendRecords([record1, record2]);
                    //  We just force the persistance to occur synchronously
                    await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);
                }

                Assert.Equal(2 * LOOP_SIZE, db.PrimitiveTable.Query().Count());

                //  Delete all even numbers
                db.PrimitiveTable.Query()
                    .Where(pf => pf.In(
                        r => r.Integer,
                        Enumerable.Range(0, LOOP_SIZE).Select(i => 2 * i)))
                    .Delete();

                Assert.Equal(LOOP_SIZE, db.PrimitiveTable.Query().Count());

                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(LOOP_SIZE, db.PrimitiveTable.Query().Count());

                var metaTableName = db.Database.GetDatabaseStateSnapshot()
                    .TableMap[db.PrimitiveTable.Schema.TableName]
                    .MetadataTableName;

                Assert.NotNull(metaTableName);

                var metaTable = db.Database.GetDatabaseStateSnapshot()
                    .TableMap[metaTableName]
                    .Table;

                Assert.Equal(1, metaTable.Query().Count());
            }
        }

        [Fact]
        public async Task RacingConditionInMemory()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);

                db.PrimitiveTable.AppendRecord(record1);
                db.PrimitiveTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);

                Assert.Equal(2, db.PrimitiveTable.Query().Count());

                var tx1 = db.Database.CreateTransaction();
                var tx2 = db.Database.CreateTransaction();

                //  In tx1, we delete first one, in tx2, we delete both
                db.PrimitiveTable.Query(tx1)
                    .Where(pf => pf.Equal(r => r.Integer, record1.Integer))
                    .Delete();
                db.PrimitiveTable.Query(tx2)
                    .Delete();

                tx1.Complete();

                //  As tx1 is completed, record1 disappear
                Assert.Equal(1, db.PrimitiveTable.Query().Count());

                //  This will leave only one record, which forces the record to be in-memory
                //  (We don't persist a block with one record)
                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(1, db.PrimitiveTable.Query().Count());

                tx2.Complete();
                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(0, db.PrimitiveTable.Query().Count());
                using (var tx = db.Database.CreateTransaction())
                {
                    Assert.False(
                        tx.TransactionState.ListBlocks(
                            db.PrimitiveTable.Schema.TableName, false, false).Any());
                }
            }
        }


        [Fact]
        public async Task RacingConditionOnDisk()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {   //  We insert 4 and delete 2, ensuring that 2 can live in a block and won't
                //  be demoted to in-memory
                var record1 = new TestDatabase.Primitives(1);
                var record2 = new TestDatabase.Primitives(2);
                var record3 = new TestDatabase.Primitives(3);
                var record4 = new TestDatabase.Primitives(4);

                db.PrimitiveTable.AppendRecord(record1);
                db.PrimitiveTable.AppendRecord(record2);
                db.PrimitiveTable.AppendRecord(record3);
                db.PrimitiveTable.AppendRecord(record4);
                await db.Database.ForceDataManagementAsync(DataManagementActivity.PersistAllNonMetaData);

                Assert.Equal(4, db.PrimitiveTable.Query().Count());

                var tx1 = db.Database.CreateTransaction();
                var tx2 = db.Database.CreateTransaction();

                //  In tx1, we delete first one, in tx2, we delete the second
                db.PrimitiveTable.Query(tx1)
                    .Where(pf => pf.Equal(r => r.Integer, record1.Integer))
                    .Delete();
                db.PrimitiveTable.Query(tx2)
                    .Where(pf => pf.Equal(r => r.Integer, record2.Integer))
                    .Delete();

                tx1.Complete();

                //  As tx1 is completed, record1 disappear
                Assert.Equal(3, db.PrimitiveTable.Query().Count());

                //  This should re-write a block by hard deleting record1
                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(3, db.PrimitiveTable.Query().Count());

                tx2.Complete();
                await db.Database.ForceDataManagementAsync(DataManagementActivity.HardDeleteAll);

                Assert.Equal(2, db.PrimitiveTable.Query().Count());
                using (var tx = db.Database.CreateTransaction())
                {   //  Should be no in-memory records
                    Assert.False(
                        tx.TransactionState.ListBlocks(
                            db.PrimitiveTable.Schema.TableName, false, false).Any());
                }
            }
        }
    }
}