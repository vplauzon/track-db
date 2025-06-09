using Ipdb.Lib;
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

            table.AppendDocuments(doc);
        }

        [Fact]
        public async Task InsertAndRetrieve()
        {
            var table = await CreateTableAsync();
            var doc = new MyDocument(1, "House");

            table.AppendDocuments(doc);
            
            var retrievedDocs = table
                .Query(d => d.Id == 1)
                .ToImmutableArray();

            Assert.Single(retrievedDocs);
            Assert.Equal(doc.Id, retrievedDocs[0].Id);
            Assert.Equal(doc.Text, retrievedDocs[0].Text);
        }

        [Fact]
        public async Task InsertUpdateAndRetrieve()
        {
            var table = await CreateTableAsync();
            var doc1 = new MyDocument(1, "House");
            var doc2 = new MyDocument(1, "Home");

            table.AppendDocuments(doc1);
            table.AppendDocuments(doc2);

            var retrievedDocs = table
                .Query(d => d.Id == 1)
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
                .AddTable(TABLE_NAME, TableSchema<MyDocument>.CreateSchema(d => d.Id)));
            var table = db.GetTable<MyDocument>(TABLE_NAME);
            return table;
        }
    }
}
