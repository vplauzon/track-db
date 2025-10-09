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
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var resultsAll = db.CompoundKeyTable.Query()
                    .ToImmutableList();

                Assert.Equal(records.Length, resultsAll.Count);
                foreach(var r in records)
                {
                    Assert.Contains(r, resultsAll);
                }
            }
        }
    }
}