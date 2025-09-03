using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class AppendAndDeleteTest
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task OneRecord(bool doPushPendingData, bool doHardDelete)
        {
            await using (var db = new TestDatabase())
            {
                var record = new TestDatabase.Primitives(1);

                db.PrimitiveTable.AppendRecord(record);
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
                db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.Equal(r => r.Integer, 1))
                    .Delete();
                await db.ForceDataManagementAsync(doHardDelete
                    ? DataManagementActivity.HardDeleteAll
                    : DataManagementActivity.None);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task MultipleRecords(bool doPushPendingData)
        {
            await using (var db = new TestDatabase())
            {
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(1));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(2));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(3));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(4));
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.Equal(r => r.Integer, 1))
                    .Delete();
            }
        }
    }
}