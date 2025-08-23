using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2.DbTests
{
    public class AppendOneRecordAndDeleteTest
    {
        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task IntOnly(bool doPushPendingData)
        {
            await using (var testTable = DbTestTables.CreateIntOnly())
            {
                var record = new DbTestTables.IntOnly(1);

                testTable.Table.AppendRecord(record);
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);
                testTable.Table.Query()
                    .Where(r => r.Integer == 1)
                    .Delete();
            }
        }
    }
}