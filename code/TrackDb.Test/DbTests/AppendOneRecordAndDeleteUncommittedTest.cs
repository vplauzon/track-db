using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2.DbTests
{
    public class AppendOneRecordAndDeleteUncommittedTest
    {
        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task IntOnly(bool doPushPendingData)
        {
            await using (var testTable = DbTestTables.CreateIntOnly())
            {
                var record = new DbTestTables.IntOnly(1);

                using (var tx = testTable.Database.CreateTransaction())
                {
                    testTable.Table.AppendRecord(record, tx);
                    testTable.Table.Query(tx).Delete();
                    tx.Complete();
                }
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);

                var records = testTable.Table.Query()
                    .ToImmutableArray();

                Assert.Empty(records);
            }
        }
    }
}