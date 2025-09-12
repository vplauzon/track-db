using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class QueryTest
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task QueryOnly(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = new TestDatabase())
            {
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(1));
                await db.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(2));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(3, 43));
                await db.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var resultsAll = db.PrimitiveTable.Query()
                    .ToImmutableList();

                Assert.Equal(3, resultsAll.Count);
                Assert.Contains(1, resultsAll.Select(r => r.Integer));
                Assert.Contains(2, resultsAll.Select(r => r.Integer));
                Assert.Contains(3, resultsAll.Select(r => r.Integer));

                var resultsEqual = db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.Equal(i => i.Integer, 2))
                    .ToImmutableList();

                Assert.Single(resultsEqual);
                Assert.Equal(2, resultsEqual[0].Integer);
                Assert.Null(resultsEqual[0].NullableInteger);

                var resultsNotEqual = db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.NotEqual(i => i.Integer, 2))
                    .ToImmutableList();

                Assert.Equal(2, resultsNotEqual.Count);
                Assert.Contains(1, resultsNotEqual.Select(r => r.Integer));
                Assert.Contains(3, resultsNotEqual.Select(r => r.Integer));

                var resultsLessThan = db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.LessThan(i => i.Integer, 2))
                    .ToImmutableList();

                Assert.Single(resultsLessThan);
                Assert.Equal(1, resultsLessThan[0].Integer);

                var resultsLessThanOrEqual = db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.LessThanOrEqual(i => i.Integer, 2))
                    .ToImmutableList();

                Assert.Equal(2, resultsLessThanOrEqual.Count);
                Assert.Contains(1, resultsLessThanOrEqual.Select(r => r.Integer));
                Assert.Contains(2, resultsLessThanOrEqual.Select(r => r.Integer));

                var resultsGreaterThan = db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.GreaterThan(i => i.Integer, 2))
                    .ToImmutableList();

                Assert.Single(resultsGreaterThan);
                Assert.Equal(3, resultsGreaterThan[0].Integer);
                Assert.Equal(43, resultsGreaterThan[0].NullableInteger);

                var resultsGreaterThanOrEqual = db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.GreaterThanOrEqual(i => i.Integer, 2))
                    .ToImmutableList();

                Assert.Equal(2, resultsGreaterThanOrEqual.Count);
                Assert.Contains(2, resultsGreaterThanOrEqual.Select(r => r.Integer));
                Assert.Contains(3, resultsGreaterThanOrEqual.Select(r => r.Integer));
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task QueryOnlyCompoundKey(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = new TestDatabase())
            {
                var record1 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(
                        new TestDatabase.FullName("Albain", "Forn"),
                        78),
                    12);
                var record2 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(
                        new TestDatabase.FullName("Sylverter", "Magnus"),
                        15),
                    25);
                var record3 = new TestDatabase.CompoundKeys(
                    new TestDatabase.VersionedName(
                        new TestDatabase.FullName("Yulnick", "Barn"),
                        897),
                    1562);

                db.CompoundKeyTable.AppendRecord(record1);
                await db.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
                db.CompoundKeyTable.AppendRecord(record2);
                db.CompoundKeyTable.AppendRecord(record3);
                await db.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var resultsAll = db.CompoundKeyTable.Query()
                    .ToImmutableList();

                Assert.Equal(3, resultsAll.Count);
                Assert.Contains(
                    record1.VersionedName.FullName.FirstName,
                    resultsAll.Select(r => r.VersionedName.FullName.FirstName));
                Assert.Contains(
                    record2.VersionedName.FullName.FirstName,
                    resultsAll.Select(r => r.VersionedName.FullName.FirstName));
                Assert.Contains(
                    record3.VersionedName.FullName.FirstName,
                    resultsAll.Select(r => r.VersionedName.FullName.FirstName));

                var resultsEqualLeaf = db.CompoundKeyTable.Query()
                    .Where(db.CompoundKeyTable.PredicateFactory.Equal(
                        r => r.VersionedName.FullName.FirstName,
                        record2.VersionedName.FullName.FirstName))
                    .ToImmutableList();

                Assert.Single(resultsEqualLeaf);
                Assert.Equal(
                    record2.VersionedName.FullName.LastName,
                    resultsEqualLeaf[0].VersionedName.FullName.LastName);

                var resultsNotEqualLeaf = db.CompoundKeyTable.Query()
                    .Where(db.CompoundKeyTable.PredicateFactory.NotEqual(
                        r => r.VersionedName.FullName.FirstName,
                        record2.VersionedName.FullName.FirstName))
                    .ToImmutableList();

                Assert.Equal(2, resultsNotEqualLeaf.Count);
                Assert.Contains(record1.Value, resultsNotEqualLeaf.Select(r => r.Value));
                Assert.Contains(record3.Value, resultsNotEqualLeaf.Select(r => r.Value));
            }
        }

        [Theory]
        [InlineData(false, false, false)]
        [InlineData(true, false, false)]
        [InlineData(true, true, false)]
        [InlineData(true, true, true)]
        [InlineData(false, true, true)]
        [InlineData(false, false, true)]
        public async Task IntOnly(bool doPushPendingData1, bool doPushPendingData2, bool doPushPendingData3)
        {
            await using (var db = new TestDatabase())
            {
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(1));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(2));
                
                Assert.Equal(2, db.PrimitiveTable.Query().Count());
                
                await db.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                Assert.Equal(2, db.PrimitiveTable.Query().Count());

                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(3));

                Assert.Equal(3, db.PrimitiveTable.Query().Count());

                await db.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                Assert.Equal(3, db.PrimitiveTable.Query().Count());

                //  Delete
                db.PrimitiveTable.Query()
                    .Where(db.PrimitiveTable.PredicateFactory.Equal(r => r.Integer, 2))
                    .Delete();
                await db.ForceDataManagementAsync(doPushPendingData3
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var resultsAll = db.PrimitiveTable.Query()
                    .ToImmutableList();

                Assert.Equal(2, resultsAll.Count);
                Assert.Contains(1, resultsAll.Select(r => r.Integer));
                Assert.Contains(3, resultsAll.Select(r => r.Integer));

                var resultsEqual = db.PrimitiveTable.Query()
                    .Where(i => i.Integer == 2)
                    .ToImmutableList();

                Assert.Empty(resultsEqual);

                var resultsNotEqual = db.PrimitiveTable.Query()
                    .Where(i => i.Integer != 2)
                    .ToImmutableList();

                Assert.Equal(2, resultsNotEqual.Count);
                Assert.Contains(1, resultsNotEqual.Select(r => r.Integer));
                Assert.Contains(3, resultsNotEqual.Select(r => r.Integer));

                var resultsLessThan = db.PrimitiveTable.Query()
                    .Where(i => i.Integer < 2)
                    .ToImmutableList();

                Assert.Single(resultsLessThan);
                Assert.Equal(1, resultsLessThan[0].Integer);

                var resultsLessThanOrEqual = db.PrimitiveTable.Query()
                    .Where(i => i.Integer <= 2)
                    .ToImmutableList();

                Assert.Single(resultsLessThanOrEqual);
                Assert.Contains(1, resultsLessThanOrEqual.Select(r => r.Integer));

                var resultsGreaterThan = db.PrimitiveTable.Query()
                    .Where(i => i.Integer > 2)
                    .ToImmutableList();

                Assert.Single(resultsGreaterThan);
                Assert.Equal(3, resultsGreaterThan[0].Integer);

                var resultsGreaterThanOrEqual = db.PrimitiveTable.Query()
                    .Where(i => i.Integer >= 2)
                    .ToImmutableList();

                Assert.Single(resultsGreaterThanOrEqual);
                Assert.Contains(3, resultsGreaterThanOrEqual.Select(r => r.Integer));
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task QueryCount(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = new TestDatabase())
            {
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(1));
                await db.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(2));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(3, 43));
                await db.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var count = db.PrimitiveTable.Query()
                    .Count();

                Assert.Equal(3, count);
            }
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task QueryWithTake(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = new TestDatabase())
            {
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(1));
                await db.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(2));
                db.PrimitiveTable.AppendRecord(new TestDatabase.Primitives(3, 43));
                await db.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var resultsOnly2 = db.PrimitiveTable.Query()
                    .Take(2)
                    .ToImmutableList();

                Assert.Equal(2, resultsOnly2.Count);
            }
        }
    }
}