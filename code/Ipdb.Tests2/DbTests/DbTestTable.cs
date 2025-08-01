using Ipdb.Lib2;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Tests2.DbTests
{
    internal class DbTestTable<T> : IAsyncDisposable
        where T : notnull
    {
        public DbTestTable(TypedTableSchema<T> schema)
        {
            Database = new Database(
                Path.Combine(
                    Environment.GetEnvironmentVariable("DbRoot")!,
                    $"{DateTime.Now:yyyy-MM-dd_HH-mm-ss-fff}"),
                schema);
            Table = Database.GetTypedTable<T>(schema.TableName);
        }

        public Database Database { get; }

        public TypedTable<T> Table { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)Database).DisposeAsync();
        }
    }
}
