using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib
{
    public class DatabaseSchema
    {
        #region Constructor
        public DatabaseSchema()
            : this(
                  ImmutableDictionary<string, object>.Empty,
                  ImmutableDictionary<string, IImmutableList<string>>.Empty)
        {
        }

        private DatabaseSchema(
            IImmutableDictionary<string, object> tableSchemaMap,
            IImmutableDictionary<string, IImmutableList<string>> tableIndexMap)
        {
            TableSchemaMap = tableSchemaMap;
            TableIndexMap = tableIndexMap;
        }
        #endregion

        public DatabaseSchema AddTable<T>(string tableName, TableSchema<T> schema)
        {
            return new DatabaseSchema(
                TableSchemaMap.Add(tableName, schema),
                TableIndexMap.Add(
                    tableName,
                    schema.Indexes.Select(i => i.PropertyPath).ToImmutableArray()));
        }

        internal IImmutableDictionary<string, object> TableSchemaMap { get; }

        internal IImmutableDictionary<string, IImmutableList<string>> TableIndexMap { get; }
    }
}
