using Ipdb.Lib;
using Ipdb.Lib.Querying;
using System;
using System.Collections.Immutable;
using System.Net;
using System.Threading.Tasks;

namespace Ipdb.Tests.PrimaryIndex
{
    public class IntIndexTest : BaseTest
    {
        #region Inner types
        private record MyDocument(int Id, string Text);
        #endregion

        private const string TABLE_NAME = "mydocs";

        [Fact]
        public async Task InsertOnly()
        {
            var table = await CreateTableAsync();
            var doc = new MyDocument(1, "House");

            table.AppendDocument(doc);
        }

        [Fact]
        public async Task InsertAndRetrieve()
        {
            var table = await CreateTableAsync();
            var doc = new MyDocument(42, "House");

            table.AppendDocument(doc);

            var retrievedDocs = table
                .Query(table.QueryOp.Equal(d => d.Id, doc.Id))
                .ToImmutableArray();

            Assert.Single(retrievedDocs);
            Assert.Equal(doc, retrievedDocs[0]);
        }

        [Fact]
        public async Task InsertUpdateAndRetrieve()
        {
            var table = await CreateTableAsync();
            var doc1 = new MyDocument(1, "House");
            var doc2 = new MyDocument(1, "Home");

            table.AppendDocument(doc1);
            table.AppendDocument(doc2);

            var retrievedDocs = table
                .Query(table.QueryOp.Equal(d => d.Id, 1))
                .ToImmutableArray();

            Assert.Single(retrievedDocs);
            Assert.Equal(doc2.Id, retrievedDocs[0].Id);
            Assert.Equal(doc2.Text, retrievedDocs[0].Text);
        }

        private async Task<Table<MyDocument>> CreateTableAsync()
        {
            var db = await Engine.LoadDatabaseAsync(
                "mydb",
                new DatabaseSchema()
                .AddTable(TableSchema<MyDocument>.CreateSchema(TABLE_NAME, d => d.Id)));
            var table = db.GetTable<MyDocument>(TABLE_NAME);

            return table;
        }
    }
}
