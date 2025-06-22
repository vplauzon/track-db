using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2
{
    /// <summary>
    /// Database:  a collection of tables that can share transactions
    /// and are persisted in the same file.
    /// </summary>
    public class Database
    {
        #region Constructors
        public Database(string databaseRootDirectory, params IEnumerable<TableSchema> schemas)
        {
            var q = schemas
                .Select(s =>new
                {
                    Table = CreateTable(s),
                    s.TableName
                });
        }

        private object CreateTable(TableSchema schema)
        {
            var tableType = typeof(Table<>).MakeGenericType(schema.RepresentationType);
            var table = Activator.CreateInstance(
                tableType,
                BindingFlags.Instance | BindingFlags.NonPublic,
                null,
                [schema],
                null);

            return table!;
        }
        #endregion

        public Table<T> GetTable<T>(string tableName)
        {
            throw new NotImplementedException();
        }

        public async Task PersistAllDataAsync()
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}