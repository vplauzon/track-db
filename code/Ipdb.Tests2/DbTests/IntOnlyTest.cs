using Ipdb.Lib2;
using System;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Ipdb.Tests2.DbTests
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
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);
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
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);
            }
        }

        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task AppendOneRecordAndDelete(bool doPushPendingData)
        {
            await using (var testTable = CreateTestTable())
            {
                var record = new IntOnly(1);

                testTable.Table.AppendRecord(record);
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);
                testTable.Table.Query()
                    .Where(r => r.Integer == 1)
                    .Delete();
            }
        }

        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task AppendMultipleRecordsAndDelete(bool doPushPendingData)
        {
            await using (var testTable = CreateTestTable())
            {
                testTable.Table.AppendRecord(new IntOnly(1));
                testTable.Table.AppendRecord(new IntOnly(2));
                testTable.Table.AppendRecord(new IntOnly(3));
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);
                testTable.Table.Query()
                    .Delete();
            }
        }

        [Theory]
        [InlineData(false)]
        //[InlineData(true)]
        public async Task Query(bool doPushPendingData)
        {
            await using (var testTable = CreateTestTable())
            {
                testTable.Table.AppendRecord(new IntOnly(1));
                testTable.Table.AppendRecord(new IntOnly(2));
                testTable.Table.AppendRecord(new IntOnly(3));
                await testTable.Database.ForceDataManagementAsync(doPushPendingData);

                var resultsEqual = testTable.Table.Query()
                    .Where(i => i.Integer == 2)
                    .ToImmutableList();

                Assert.Single(resultsEqual);
                Assert.Equal(2, resultsEqual[0].Integer);

                var resultsNotEqual = testTable.Table.Query()
                    .Where(i => i.Integer != 2)
                    .ToImmutableList();

                Assert.Equal(2, resultsNotEqual.Count);
                Assert.Contains(1, resultsNotEqual.Select(r => r.Integer));
                Assert.Contains(3, resultsNotEqual.Select(r => r.Integer));

                var resultsLessThan = testTable.Table.Query()
                    .Where(i => i.Integer < 2)
                    .ToImmutableList();

                Assert.Single(resultsLessThan);
                Assert.Equal(1, resultsLessThan[0].Integer);

                var resultsLessThanOrEqual = testTable.Table.Query()
                    .Where(i => i.Integer <= 2)
                    .ToImmutableList();

                Assert.Equal(2, resultsLessThanOrEqual.Count);
                Assert.Contains(1, resultsLessThanOrEqual.Select(r => r.Integer));
                Assert.Contains(2, resultsLessThanOrEqual.Select(r => r.Integer));

                var resultsGreaterThan = testTable.Table.Query()
                    .Where(i => i.Integer > 2)
                    .ToImmutableList();

                Assert.Single(resultsGreaterThan);
                Assert.Equal(3, resultsGreaterThan[0].Integer);

                var resultsGreaterThanOrEqual = testTable.Table.Query()
                    .Where(i => i.Integer >= 2)
                    .ToImmutableList();

                Assert.Equal(2, resultsGreaterThanOrEqual.Count);
                Assert.Contains(2, resultsGreaterThanOrEqual.Select(r => r.Integer));
                Assert.Contains(3, resultsGreaterThanOrEqual.Select(r => r.Integer));
            }
        }

        private DbTestTable<IntOnly> CreateTestTable()
        {
            return new DbTestTable<IntOnly>(new TableSchema<IntOnly>(TABLE_NAME));
        }
    }
}