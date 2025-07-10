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
        public async Task AppendOneRecord(bool doPushPendingData)
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

        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task AppendMultipleRecords(bool doPushPendingData)
        {
            await using (var testTable = CreateTestTable())
            {
                testTable.Table.AppendRecord(new IntOnly(1));
                testTable.Table.AppendRecord(new IntOnly(2));
                testTable.Table.AppendRecord(new IntOnly(3));
                if (doPushPendingData)
                {
                    await testTable.Database.PersistAllDataAsync();
                }
            }
        }

        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task QueryEqual(bool doPushPendingData)
        {
            await using (var testTable = CreateTestTable())
            {
                testTable.Table.AppendRecord(new IntOnly(1));
                testTable.Table.AppendRecord(new IntOnly(2));
                testTable.Table.AppendRecord(new IntOnly(3));
                if (doPushPendingData)
                {
                    await testTable.Database.PersistAllDataAsync();
                }

                var results = testTable.Table.Query(i => i.Integer == 2);

                Assert.Single(results);
                Assert.Equal(2, results[0].Integer);
            }
        }

        private TestTable<IntOnly> CreateTestTable()
        {
            return new TestTable<IntOnly>(new TableSchema<IntOnly>(TABLE_NAME));
        }
    }
}