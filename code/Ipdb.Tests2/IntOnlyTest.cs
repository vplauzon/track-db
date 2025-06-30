using Ipdb.Lib2;
using System;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2
{
    public class IntOnlyTest
    {
        #region Inner types
        private record IntOnly(int Integer);
        #endregion

        private const string TABLE_NAME = "ints";

        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task AppendOnly(bool doPushPendingData)
        {
            await using (var testTable = CreateTestTable())
            {
                var record = new IntOnly(1);

                testTable.Table.AppendRecord(record);
                if (doPushPendingData)
                {
                    await testTable.Database.PersistAllDataAsync();
                }
            }
        }

        private TestTable<IntOnly> CreateTestTable()
        {
            return new TestTable<IntOnly>(
                new TableSchema<IntOnly>(TABLE_NAME)
                .AddPrimaryKey(d => d.Integer));
        }
    }
}