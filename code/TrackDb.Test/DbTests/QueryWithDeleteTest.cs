using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class QueryWithDeleteTest
    {
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
                db.IntOnlyTable.AppendRecord(new TestDatabase.IntOnly(1));
                db.IntOnlyTable.AppendRecord(new TestDatabase.IntOnly(2));
                await db.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);
                db.IntOnlyTable.AppendRecord(new TestDatabase.IntOnly(3));
                await db.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);

                //  Delete
                db.IntOnlyTable.Query()
                    .Where(db.IntOnlyTable.PredicateFactory.Equal(r => r.Integer, 2))
                    .Delete();
                await db.ForceDataManagementAsync(doPushPendingData3
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);

                var resultsAll = db.IntOnlyTable.Query()
                    .ToImmutableList();

                Assert.Equal(2, resultsAll.Count);
                Assert.Contains(1, resultsAll.Select(r => r.Integer));
                Assert.Contains(3, resultsAll.Select(r => r.Integer));

                var resultsEqual = db.IntOnlyTable.Query()
                    .Where(i => i.Integer == 2)
                    .ToImmutableList();

                Assert.Empty(resultsEqual);

                var resultsNotEqual = db.IntOnlyTable.Query()
                    .Where(i => i.Integer != 2)
                    .ToImmutableList();

                Assert.Equal(2, resultsNotEqual.Count);
                Assert.Contains(1, resultsNotEqual.Select(r => r.Integer));
                Assert.Contains(3, resultsNotEqual.Select(r => r.Integer));

                var resultsLessThan = db.IntOnlyTable.Query()
                    .Where(i => i.Integer < 2)
                    .ToImmutableList();

                Assert.Single(resultsLessThan);
                Assert.Equal(1, resultsLessThan[0].Integer);

                var resultsLessThanOrEqual = db.IntOnlyTable.Query()
                    .Where(i => i.Integer <= 2)
                    .ToImmutableList();

                Assert.Single(resultsLessThanOrEqual);
                Assert.Contains(1, resultsLessThanOrEqual.Select(r => r.Integer));

                var resultsGreaterThan = db.IntOnlyTable.Query()
                    .Where(i => i.Integer > 2)
                    .ToImmutableList();

                Assert.Single(resultsGreaterThan);
                Assert.Equal(3, resultsGreaterThan[0].Integer);

                var resultsGreaterThanOrEqual = db.IntOnlyTable.Query()
                    .Where(i => i.Integer >= 2)
                    .ToImmutableList();

                Assert.Single(resultsGreaterThanOrEqual);
                Assert.Contains(3, resultsGreaterThanOrEqual.Select(r => r.Integer));
            }
        }
    }
}