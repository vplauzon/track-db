using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class OrderByTest
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
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(1, 2222, 74, 4));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 222, 205, 98));
                await db.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 22, 14, -4));
                db.MultiIntegerTable.AppendRecord(new TestDatabase.MultiIntegers(11, 22, -89, 44));
                await db.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);

                var results = db.MultiIntegerTable.Query()
                    .OrderByDesc(m => m.Integer1)
                    .ThenBy(m => m.Integer2)
                    .ThenByDesc(m => m.Integer4)
                    .Take(3)
                    .ToImmutableList();

                Assert.Equal(3, results.Count);
                
                Assert.Equal(11, results[0].Integer1);
                Assert.Equal(11, results[1].Integer1);
                Assert.Equal(11, results[2].Integer1);

                Assert.Equal(22, results[0].Integer2);
                Assert.Equal(22, results[1].Integer2);
                Assert.Equal(222, results[2].Integer2);

                Assert.Equal(44, results[0].Integer4);
                Assert.Equal(-4, results[1].Integer4);
                Assert.Equal(98, results[2].Integer4);

                Assert.Equal(-89, results[0].Integer3);
                Assert.Equal(14, results[1].Integer3);
                Assert.Equal(205, results[2].Integer3);
            }
        }
    }
}