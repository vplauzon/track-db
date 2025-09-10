using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class AppendOnlyTest
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task OneRecord(bool doPushPendingData)
        {
            await using (var db = new TestDatabase())
            {
                var record = new TestDatabase.Primitives(1);

                db.PrimitiveTable.AppendRecord(record);
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task OneCompoundKeyRecord(bool doPushPendingData)
        {
            await using (var db = new TestDatabase())
            {
                var record = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedId("Bob", 3),
                    1);

                db.CompoundKeyTable.AppendRecord(record);
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
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
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
            }
        }
    }
}