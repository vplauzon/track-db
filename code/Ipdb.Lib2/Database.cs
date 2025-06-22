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
        private readonly IImmutableDictionary<string, object> _tableMap
            = ImmutableDictionary<string, object>.Empty;

        #region Constructors
        public Database(string databaseRootDirectory, params IEnumerable<TableSchema> schemas)
        {
            _tableMap = schemas
                .Select(s => new
                {
                    Table = CreateTable(s),
                    s.TableName
                })
                .ToImmutableDictionary(o => o.TableName, o => o.Table);
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
            if (_tableMap.ContainsKey(tableName))
            {
                var table = _tableMap[tableName];

                if (table is Table<T> t)
                {
                    return t;
                }
                else
                {
                    var docType = table.GetType().GetGenericArguments().First();

                    throw new InvalidOperationException(
                        $"Table '{tableName}' doesn't have document type '{typeof(T).Name}':  " +
                        $"it has document type '{docType.Name}'");
                }
            }
            else
            {
                throw new InvalidOperationException($"Table '{tableName}' doesn't exist");
            }
        }

        public async Task PersistAllDataAsync()
        {
            await Task.CompletedTask;

            throw new NotImplementedException();
        }
    }
}