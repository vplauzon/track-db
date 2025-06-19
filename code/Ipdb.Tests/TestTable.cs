using Ipdb.Lib;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Tests
{
    internal static class TestTable
    {
        public static async Task<TestTable<T>> CreateAsync<T>(TableSchema<T> schema)
        {
            var testTable = await TestTable<T>.CreateAsync(schema);

            return testTable;
        }
    }
    internal class TestTable<T> : IAsyncDisposable
    {
        #region Constructors
        public static async Task<TestTable<T>> CreateAsync(TableSchema<T> schema)
        {
            var engine = new Engine(Path.Combine(
                Environment.GetEnvironmentVariable("EngineRoot")!,
                $"{DateTime.Now:yyyy-MM-dd_HH-mm-ss-fff}"));
            var database = await engine.LoadDatabaseAsync(
                "mydb",
                new DatabaseSchema().AddTable(schema));

            return new TestTable<T>(engine, database, schema.TableName);
        }

        private TestTable(Engine engine, Database database, string tableName)
        {
            Engine = engine;
            Database = database;
            Table = database.GetTable<T>(tableName);
        }
        #endregion

        public Engine Engine { get; }

        public Database Database { get; }

        public Table<T> Table { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Engine).DisposeAsync();
        }
    }
}
