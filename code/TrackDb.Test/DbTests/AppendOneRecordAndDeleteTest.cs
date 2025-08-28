using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Tests.DbTests
{
    public class AppendOneRecordAndDeleteTest
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task IntOnly(bool doPushPendingData, bool doHardDelete)
        {
            await using (var testTable = DbTestTables.CreateIntOnly())
            {
                var record = new DbTestTables.IntOnly(1);

                testTable.Table.AppendRecord(record);
                await testTable.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);
                testTable.Table.Query()
                    .Where(testTable.Table.PredicateFactory.Equal(r => r.Integer, 1))
                    .Delete();
                await testTable.Database.ForceDataManagementAsync(doHardDelete
                    ? DataManagementActivity.HardDeleteAll
                    : DataManagementActivity.None);
            }
        }
    }
}