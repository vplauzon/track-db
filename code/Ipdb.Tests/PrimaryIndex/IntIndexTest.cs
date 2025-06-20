using Ipdb.Lib;
using System;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Ipdb.Tests.PrimaryIndex
{
    public class IntIndexTest
    {
        #region Inner types
        private record MyDocument(int Id, string Text);
        #endregion

        private const string TABLE_NAME = "mydocs";

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task InsertOnly(bool doPushPendingData)
        {
            await using (var testTable = await CreateTestTableAsync())
            {
                var doc = new MyDocument(1, "House");

                testTable.Table.AppendDocument(doc);
                if (doPushPendingData)
                {
                    await testTable.Database.PushPendingDataAsync();
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task InsertAndRetrieveAsync(bool doPushPendingData)
        {
            await using (var testTable = await CreateTestTableAsync())
            {
                var doc = new MyDocument(42, "House");

                testTable.Table.AppendDocument(doc);
                if (doPushPendingData)
                {
                    await testTable.Database.PushPendingDataAsync();
                }

                var retrievedDocs = testTable.Table
                    .Query(testTable.Table.QueryOp.Equal(d => d.Id, doc.Id))
                    .ToImmutableArray();

                Assert.Single(retrievedDocs);
                Assert.Equal(doc, retrievedDocs[0]);
            }
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task InsertUpdateAndRetrieveAsync(
            bool doPushPendingData1,
            bool doPushPendingData2)
        {
            await using (var testTable = await CreateTestTableAsync())
            {
                var doc1 = new MyDocument(1, "House");
                var doc2 = new MyDocument(1, "Home");

                testTable.Table.AppendDocument(doc1);
                if (doPushPendingData1)
                {
                    await testTable.Database.PushPendingDataAsync();
                }
                testTable.Table.AppendDocument(doc2);
                if (doPushPendingData2)
                {
                    await testTable.Database.PushPendingDataAsync();
                }

                var retrievedDocs = testTable.Table
                    .Query(testTable.Table.QueryOp.Equal(d => d.Id, 1))
                    .ToImmutableArray();

                Assert.Single(retrievedDocs);
                Assert.Equal(doc2.Id, retrievedDocs[0].Id);
                Assert.Equal(doc2.Text, retrievedDocs[0].Text);
            }
        }

        private async Task<TestTable<MyDocument>> CreateTestTableAsync()
        {
            return await TestTable.CreateAsync(
                TableSchema<MyDocument>.CreateSchema(TABLE_NAME, d => d.Id));
        }
    }
}