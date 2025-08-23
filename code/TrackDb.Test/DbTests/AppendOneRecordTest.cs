using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace TrackDb.Tests.DbTests
{
    public class AppendOneRecordTest
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task IntOnly(bool doPushPendingData)
        {
            await using (var testTable = DbTestTables.CreateIntOnly())
            {
                var record = new DbTestTables.IntOnly(1);

                testTable.Table.AppendRecord(record);
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);
            }
        }
    }
}