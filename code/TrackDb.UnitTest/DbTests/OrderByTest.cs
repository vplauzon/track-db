using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.UnitTest.DbTests
{
    public class OrderByTest
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task TopOne(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(4, 1, 1, 1));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(2, 1, 1, 1));
                await db.Database.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(1, 1, 1, 1));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(5, 1, 1, 1));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(3, 1, 1, 1));
                await db.Database.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var results = db.MultiIntegerTable.Query()
                    .OrderBy(m => m.Integer1)
                    .Take(1)
                    .ToImmutableList();

                Assert.Single(results);
                Assert.Equal(1, results[0].Integer1);
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task WithNulls(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(1, null));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(2, 5));
                await db.Database.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(3, null));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(4, 6));
                await db.Database.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var results = db.PrimitiveTable.Query()
                    .OrderBy(m => m.NullableInteger)
                    .Take(3)
                    .ToImmutableList();

                Assert.Equal(3, results.Count);
                Assert.Null(results[0].NullableInteger);
                Assert.Null(results[1].NullableInteger);
                Assert.Equal(5, results[2].NullableInteger);
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task QueryOnly(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(1, 2222, 74, 4));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 222, 205, 98));
                await db.Database.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 22, 14, -4));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 22, -89, 44));
                await db.Database.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var results = db.MultiIntegerTable.Query()
                    .OrderByDesc(m => m.Integer1)
                    .ThenBy(m => m.Integer2)
                    .ThenByDesc(m => m.Integer4)
                    .Take(3)
                    .ToImmutableList();
                //  We should get:
                //  (11, 22, -89, 44)
                //  (11, 22, 14, -4)
                //  (11, 222, 205, 98)
                //  (1, 2222, 74, 4) <-- This one taken out

                Assert.Equal(3, results.Count);

                Assert.Equal(11, results[0].Integer1);
                Assert.Equal(22, results[0].Integer2);
                Assert.Equal(-89, results[0].Integer3);
                Assert.Equal(44, results[0].Integer4);

                Assert.Equal(11, results[1].Integer1);
                Assert.Equal(22, results[1].Integer2);
                Assert.Equal(14, results[1].Integer3);
                Assert.Equal(-4, results[1].Integer4);

                Assert.Equal(11, results[2].Integer1);
                Assert.Equal(222, results[2].Integer2);
                Assert.Equal(205, results[2].Integer3);
                Assert.Equal(98, results[2].Integer4);
            }
        }


        [Theory]
        [InlineData(false, false, false)]
        [InlineData(true, false, false)]
        [InlineData(false, true, false)]
        [InlineData(true, true, false)]
        [InlineData(true, false, true)]
        [InlineData(false, true, true)]
        [InlineData(true, true, true)]
        public async Task WithDeletes(
            bool doPushPendingData1,
            bool doPushPendingData2,
            bool doHardDelete)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(1, 2222, 74, 4));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 222, 205, 98));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(95, 1, 2, 3));
                await db.Database.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(98, 1, 2, 3));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 22, 14, -4));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 22, -89, 44));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(99, 1, 2, 3));
                await db.Database.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                //  Delete bunch of records
                db.MultiIntegerTable.Query()
                    .Where(pf => pf.GreaterThan(m => m.Integer1, 90))
                    .Delete();
                await db.Database.ForceDataManagementAsync(doHardDelete
                    ? DataManagementActivity.HardDeleteAll
                    : DataManagementActivity.None);

                var results = db.MultiIntegerTable.Query()
                    .OrderByDesc(m => m.Integer1)
                    .ThenBy(m => m.Integer2)
                    .ThenByDesc(m => m.Integer4)
                    .Take(3)
                    .ToImmutableList();
                //  We should get:
                //  (11, 22, -89, 44)
                //  (11, 22, 14, -4)
                //  (11, 222, 205, 98)
                //  (1, 2222, 74, 4) <-- This one taken out

                Assert.Equal(3, results.Count);

                Assert.Equal(11, results[0].Integer1);
                Assert.Equal(22, results[0].Integer2);
                Assert.Equal(-89, results[0].Integer3);
                Assert.Equal(44, results[0].Integer4);

                Assert.Equal(11, results[1].Integer1);
                Assert.Equal(22, results[1].Integer2);
                Assert.Equal(14, results[1].Integer3);
                Assert.Equal(-4, results[1].Integer4);

                Assert.Equal(11, results[2].Integer1);
                Assert.Equal(222, results[2].Integer2);
                Assert.Equal(205, results[2].Integer3);
                Assert.Equal(98, results[2].Integer4);
            }
        }
    }
}