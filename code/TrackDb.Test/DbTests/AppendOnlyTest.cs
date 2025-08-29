using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class AppendOnlyTest
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task OneRecord(bool doPushPendingData)
        {
            await using (var db = new TestDatabase())
            {
                var record = new TestDatabase.IntOnly(1);

                db.IntOnlyTable.AppendRecord(record);
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);
            }
        }
    }
}