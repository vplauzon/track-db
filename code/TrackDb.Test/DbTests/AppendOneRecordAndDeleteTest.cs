using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class AppendOneRecordAndDeleteTest
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task IntOnly(bool doPushPendingData, bool doHardDelete)
        {
            await using (var db = new TestDatabase())
            {
                var record = new TestDatabase.IntOnly(1);

                db.IntOnlyTable.AppendRecord(record);
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);
                db.IntOnlyTable.Query()
                    .Where(db.IntOnlyTable.PredicateFactory.Equal(r => r.Integer, 1))
                    .Delete();
                await db.ForceDataManagementAsync(doHardDelete
                    ? DataManagementActivity.HardDeleteAll
                    : DataManagementActivity.None);
            }
        }
    }
}