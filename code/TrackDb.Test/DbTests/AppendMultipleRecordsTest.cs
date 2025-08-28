using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using TrackDb.Lib;
using Xunit;

namespace TrackDb.Tests.DbTests
{
    public class AppendMultipleRecords
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task IntOnly(bool doPushPendingData)
        {
            await using (var testTable = DbTestTables.CreateIntOnly())
            {
                testTable.Table.AppendRecord(new DbTestTables.IntOnly(1));
                testTable.Table.AppendRecord(new DbTestTables.IntOnly(2));
                testTable.Table.AppendRecord(new DbTestTables.IntOnly(3));
                await testTable.Database.ForceDataManagementAsync(doPushPendingData
                    ? DataManagementActivity.PersistAllData
                    : DataManagementActivity.None);
            }
        }
    }
}