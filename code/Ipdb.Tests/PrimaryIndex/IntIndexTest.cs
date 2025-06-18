using Ipdb.Lib;
using System;
using System.Collections.Immutable;
using System.Threading.Tasks;

namespace Ipdb.Tests.PrimaryIndex
{
    public class IntIndexTest : BaseTest, IAsyncLifetime
    {
        #region Inner types
        private record MyDocument(int Id, string Text);
        #endregion

        private const string TABLE_NAME = "mydocs";
        private Database? _db = null;

        public async Task InitializeAsync()
        {
            _db = await Engine.LoadDatabaseAsync(
                "mydb",
                new DatabaseSchema()
                .AddTable(TableSchema<MyDocument>.CreateSchema(TABLE_NAME, d => d.Id)));
        }

        public Task DisposeAsync() => Task.CompletedTask;

        [Fact]
        public void InsertOnly()
        {
            var table = CreateTable();
            var doc = new MyDocument(1, "House");

            table.AppendDocument(doc);
        }

        [Fact]
        public void InsertAndRetrieve()
        {
            var table = CreateTable();
            var doc = new MyDocument(42, "House");

            table.AppendDocument(doc);

            var retrievedDocs = table
                .Query(table.QueryOp.Equal(d => d.Id, doc.Id))
                .ToImmutableArray();

            Assert.Single(retrievedDocs);
            Assert.Equal(doc, retrievedDocs[0]);
        }

        [Fact]
        public void InsertUpdateAndRetrieve()
        {
            var table = CreateTable();
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

        private Table<MyDocument> CreateTable()
        {
            return _db!.GetTable<MyDocument>(TABLE_NAME);
        }
    }
}