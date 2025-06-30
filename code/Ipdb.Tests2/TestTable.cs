using Ipdb.Lib2;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Tests2
{
    internal class TestTable<T> : IAsyncDisposable
        where T : notnull
    {
        public TestTable(TableSchema<T> schema)
        {
            Database = new Database(
                Path.Combine(
                    Environment.GetEnvironmentVariable("DbRoot")!,
                    $"{DateTime.Now:yyyy-MM-dd_HH-mm-ss-fff}"),
                schema);
            Table = Database.GetTable<T>(schema.TableName);
        }

        public Database Database { get; }

        public Table<T> Table { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
        }
    }
}
