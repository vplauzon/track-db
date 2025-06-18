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

        [Fact]
        public async Task InsertOnly()
        {
            using (var testTable = await CreateTestTableAsync())
            {
                var doc = new MyDocument(1, "House");

                testTable.Table.AppendDocument(doc);
            }
        }

        [Fact]
        public async Task InsertAndRetrieveAsync()
        {
            using (var testTable = await CreateTestTableAsync())
            {
                var doc = new MyDocument(42, "House");

                testTable.Table.AppendDocument(doc);

                var retrievedDocs = testTable.Table
                    .Query(testTable.Table.QueryOp.Equal(d => d.Id, doc.Id))
                    .ToImmutableArray();

                Assert.Single(retrievedDocs);
                Assert.Equal(doc, retrievedDocs[0]);
            }
        }

        [Fact]
        public async Task InsertUpdateAndRetrieveAsync()
        {
            using (var testTable = await CreateTestTableAsync())
            {
                var doc1 = new MyDocument(1, "House");
                var doc2 = new MyDocument(1, "Home");

                testTable.Table.AppendDocument(doc1);
                testTable.Table.AppendDocument(doc2);

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