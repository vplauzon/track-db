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
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record = new TestDatabase.Primitives(1);

                db.PrimitiveTable.AppendRecord(record);
                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task OneCompoundKeyRecord(bool doPushPendingData)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(new TestDatabase.FullName("Bob", "Saint-Clar"), 3),
                    1);

                db.CompoundKeyTable.AppendRecord(record);
                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
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
                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task MultipleCompoundKeyRecords(bool doPushPendingData)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(new TestDatabase.FullName("Bob", "Saint-Clar"), 3),
                    1);
                var record2 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(new TestDatabase.FullName("Mick", "Terrible"), 47),
                    1);
                var record3 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(new TestDatabase.FullName("Zela", "Fantastic"), -23),
                    1);

                db.CompoundKeyTable.AppendRecord(record1);
                db.CompoundKeyTable.AppendRecord(record2);
                db.CompoundKeyTable.AppendRecord(record3);
                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
            }
        }
    }
}