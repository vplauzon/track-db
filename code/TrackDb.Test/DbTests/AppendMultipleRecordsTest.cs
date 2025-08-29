using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class AppendMultipleRecords
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task IntOnly(bool doPushPendingData)
        {
            await using (var db = new TestDatabase())
            {
                db.IntOnlyTable.AppendRecord(new TestDatabase.IntOnly(1));
                db.IntOnlyTable.AppendRecord(new TestDatabase.IntOnly(2));
                db.IntOnlyTable.AppendRecord(new TestDatabase.IntOnly(3));
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);
            }
        }
    }
}