using Ipdb.Lib.Indexing;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    public class Database : IDisposable
    {
        private readonly DataManager _storageManager;
        private readonly IImmutableDictionary<string, object> _tableMap
            = ImmutableDictionary<string, object>.Empty;

        #region Constructor
        internal Database(string databaseRootDirectory, DatabaseSchema databaseSchema)
        {
            var tableMap = ImmutableDictionary<string, object>.Empty.ToBuilder();
            var tableIndexKeys = databaseSchema.TableSchemas
                .Select(s => s.IndexObjects.Select(
                    i => new TableIndexKey(s.TableName, i.PropertyPath)))
                .SelectMany(l => l)
                .ToImmutableArray();

            _storageManager = new(databaseRootDirectory, tableIndexKeys);
            foreach (var tableSchema in databaseSchema.TableSchemas)
            {
                var tableType = typeof(Table<>).MakeGenericType(tableSchema.DocumentType);
                var table = Activator.CreateInstance(
                    tableType,
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    [tableSchema, _storageManager],
                    null);

                tableMap.Add(tableSchema.TableName, table!);
            }
            _tableMap = tableMap.ToImmutableDictionary();
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

        public TransactionContext CreateTransaction()
        {
            return _storageManager.CreateTransaction();
        }

        void IDisposable.Dispose()
        {
            ((IDisposable)_storageManager).Dispose();
        }
    }
}
