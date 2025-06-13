using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    public class Database : IDisposable
    {
        private readonly DataManager _storageManager;
        private readonly IImmutableDictionary<string, object> _tableMap
            = ImmutableDictionary<string, object>.Empty;

        #region Constructor
        internal Database(string databaseRootDirectory, DatabaseSchema schema)
        {
            var tableMap = ImmutableDictionary<string, object>.Empty.ToBuilder();
            var tableIndex = 0;

            _storageManager = new(databaseRootDirectory);
            foreach (var tableName in schema.TableMap.Keys)
            {
                var schemaObject = schema.TableMap[tableName];
                var docType = schemaObject.GetType().GetGenericArguments().First();
                var tableType = typeof(Table<>).MakeGenericType(docType);
                var table = Activator.CreateInstance(
                    tableType,
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    [tableIndex++, schemaObject, _storageManager],
                    null);

                tableMap.Add(tableName, table!);
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

        void IDisposable.Dispose()
        {
            ((IDisposable)_storageManager).Dispose();
        }
    }
}
