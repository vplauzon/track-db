using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.UnitTest.DbTests
{
    public class AppendAndDeleteTest
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task OneRecord(bool doPushPendingData, bool doHardDelete)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record = new TestDatabase.Primitives(1);

                db.PrimitiveTable.AppendRecord(record);
                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);
                db.PrimitiveTable.Query()
                    .Where(pf => pf.Equal(r => r.Integer, 1))
                    .Delete();
                await db.Database.ForceDataManagementAsync(doHardDelete
                    ? DataManagementActivity.HardDeleteAll
                    : DataManagementActivity.None);

                Assert.Equal(0, db.PrimitiveTable.Query().Count());
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task MultipleRecords(bool doPushPendingData)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(1));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(2));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(3));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(4));
                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                Assert.Equal(4, db.PrimitiveTable.Query().Count());

                db.PrimitiveTable.Query()
                    .Where(pf => pf.Equal(r => r.Integer, 2))
                    .Delete();

                Assert.Equal(3, db.PrimitiveTable.Query().Count());
            }
        }

        [Fact]
        public async Task WithinTransaction()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(1));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(2));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(3));

                Assert.Equal(3, db.PrimitiveTable.Query().Count());

                using (var tx = db.CreateTransaction())
                {
                    db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(4), tx);

                    Assert.Equal(4, db.PrimitiveTable.Query(tx).Count());

                    db.PrimitiveTable.Query(tx)
                        .Delete();

                    Assert.Equal(0, db.PrimitiveTable.Query(tx).Count());
                }
            }
        }
    }
}