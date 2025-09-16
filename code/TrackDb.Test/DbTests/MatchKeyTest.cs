using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class MatchKeyTest
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        public async Task AllOperators(bool doPushPendingData1, bool doPushPendingData2)
        {
            await using (var db = await TestDatabase.CreateAsync())
            {
                var record1 = new TestDatabase.MultiIntegers(1, 2, 3, 4);
                var record2 = new TestDatabase.MultiIntegers(1, 2, 3, 0);
                var record3 = new TestDatabase.MultiIntegers(1, 2, 0, 0);
                var record4 = new TestDatabase.MultiIntegers(1, 0, 0, 0);

                db.MultiIntegerTable.AppendRecord(record1);
                db.MultiIntegerTable.AppendRecord(record2);
                await db.Database.ForceDataManagementAsync(doPushPendingData1
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);
                db.MultiIntegerTable.AppendRecord(record3);
                db.MultiIntegerTable.AppendRecord(record4);
                await db.Database.ForceDataManagementAsync(doPushPendingData2
                    ? DataManagementActivity.PersistAllUserData
                    : DataManagementActivity.None);

                var sample = new TestDatabase.MultiIntegers(1, 2, 3, 4);
                var results1 = db.MultiIntegerTable.Query()
                    .Where(pf => pf.MatchKeys(
                        sample,
                        s => s.Integer1, s => s.Integer2, s => s.Integer3, s => s.Integer4))
                    .ToImmutableList();

                Assert.Single(results1);
                Assert.Equal(record1, results1[0]);

                var results2 = db.MultiIntegerTable.Query()
                    .Where(pf => pf.MatchKeys(sample, s => s.Integer1, s => s.Integer2, s => s.Integer3))
                    .ToImmutableList();

                Assert.Equal(2, results2.Count());
                Assert.Contains(record1, results2);
                Assert.Contains(record2, results2);

                var results3 = db.MultiIntegerTable.Query()
                    .Where(pf => pf.MatchKeys(sample, s => s.Integer1, s => s.Integer2))
                    .ToImmutableList();

                Assert.Equal(3, results3.Count());
                Assert.Contains(record1, results3);
                Assert.Contains(record2, results3);
                Assert.Contains(record3, results3);

                var results4 = db.MultiIntegerTable.Query()
                    .Where(pf => pf.MatchKeys(sample, s => s.Integer1))
                    .ToImmutableList();

                Assert.Equal(4, results4.Count());
                Assert.Contains(record1, results4);
                Assert.Contains(record2, results4);
                Assert.Contains(record3, results4);
                Assert.Contains(record4, results4);
            }
        }
    }
}