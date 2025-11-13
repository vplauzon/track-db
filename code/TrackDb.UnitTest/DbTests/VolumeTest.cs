using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.UnitTest.DbTests
{
    public class VolumeTest
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AppendBunch(bool doPushPendingData)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var random = new Random();
                var records = Enumerable.Range(0, 5000)
                    .Select(i => new TestDatabase.CompoundKeys(
                        new TestDatabase.VersionedName(i, new TestDatabase.FullName(
                            $"Bob{i}", $"Smith{i + 50}")),
                        (short)random.Next(10000)))
                    .ToImmutableArray();

                db.CompoundKeyTable.AppendRecords(records);
                await db.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllNonMetaData
                    : DataManagementActivity.None);

                var resultsAll = db.CompoundKeyTable.Query()
                    .ToImmutableList();

                Assert.Equal(records.Length, resultsAll.Count);
                foreach (var r in records)
                {
                    Assert.Contains(r, resultsAll);
                }
            }
        }

        [Fact]
        public async Task AppendAndQuery()
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                const int TOTAL = 10000;
                const int BATCH = 200;

                var random = new Random();
                var records = Enumerable.Range(0, TOTAL)
                    .Select(i => new TestDatabase.Primitives(
                        random.Next(20000),
                        random.Next(2) == 1 ? 42 : null))
                    .ToImmutableArray();

                for (var i = 0; i != TOTAL / BATCH; ++i)
                {
                    db.PrimitiveTable.AppendRecords(records.Skip(i * BATCH).Take(BATCH));
                    await db.Database.ForceDataManagementAsync(
                        DataManagementActivity.PersistAllNonMetaData);
                }

                var resultsAll = db.PrimitiveTable.Query()
                    .ToImmutableList();

                Assert.Equal(records.Length, resultsAll.Count);
                Assert.Equal(
                    records.Sum(r => (long)r.Integer),
                    resultsAll.Sum(r => (long)r.Integer));
            }
        }
    }
}