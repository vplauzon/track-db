using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Test.DbTests
{
    public class AppendOneRecordAndDeleteUncommittedTest
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task IntOnly(bool doPushPendingData)
        {
            await using (var db = new TestDatabase())
            {
                var record = new TestDatabase.IntOnly(1);

                using (var tx = db.CreateTransaction())
                {
                    db.IntOnlyTable.AppendRecord(record, tx);
                    db.IntOnlyTable.Query(tx).Delete();
                    tx.Complete();
                }
                await db.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);

                var records = db.IntOnlyTable.Query()
                    .ToImmutableArray();

                Assert.Empty(records);
            }
        }
    }
}