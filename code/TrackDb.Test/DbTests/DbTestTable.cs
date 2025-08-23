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
            Database = new Database(new DatabaseSettings(), schema);
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
