using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class UpdateTest
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task SimpleKey(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.MultiIntegers(10, 1, 2, 3);
                var record2 = new TestDatabase.MultiIntegers(11, 1, 2, 3);
                var record3 = new TestDatabase.MultiIntegers(12, 1, 2, 3);
                var record4 = new TestDatabase.MultiIntegers(13, 1, 2, 3);
                var newRecord2 = new TestDatabase.MultiIntegers(11, 11, 21, 31);

                db.MultiIntegerTable.AppendRecord(record1);
                db.MultiIntegerTable.AppendRecord(record2);
                db.MultiIntegerTable.AppendRecord(record3);
                await db.Database.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
                db.MultiIntegerTable.AppendRecord(record4);

                var recordDeletedCount = db.MultiIntegerTable.UpdateRecord(record2, newRecord2);

                Assert.Equal(1, recordDeletedCount);
                await db.Database.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var resultsAll = db.MultiIntegerTable.Query()
                    .ToImmutableList();

                Assert.Equal(4, resultsAll.Count);
                Assert.Contains(record1, resultsAll);
                Assert.Contains(newRecord2, resultsAll);
                Assert.Contains(record3, resultsAll);
                Assert.Contains(record4, resultsAll);
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task CompoundKey(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(1, new TestDatabase.FullName("Al", "Jordan")),
                    2000);
                var record2 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(1, new TestDatabase.FullName("Bob", "Dan")),
                    3000);
                var record3 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(1, new TestDatabase.FullName("Carl", "Fyr")),
                    4000);
                var newRecord3 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(2, new TestDatabase.FullName("Carl", "Fyr")),
                    5000);

                db.CompoundKeyTable.AppendRecord(record1);
                db.CompoundKeyTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
                db.CompoundKeyTable.AppendRecord(record3);

                var recordDeletedCount = db.CompoundKeyTable.UpdateRecord(record3, newRecord3);

                Assert.Equal(1, recordDeletedCount);
                await db.Database.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var resultsAll = db.CompoundKeyTable.Query()
                    .ToImmutableList();

                Assert.Equal(3, resultsAll.Count);
                Assert.Contains(record1, resultsAll);
                Assert.Contains(record2, resultsAll);
                Assert.Contains(newRecord3, resultsAll);
            }
        }
    }
}