using System;
using System.Collections.Immutable;

namespace Ipdb.Lib
{
    public class DatabaseSchema
    {
        #region Constructor
        public DatabaseSchema()
            : this(ImmutableDictionary<string, object>.Empty)
        {
        }

        private DatabaseSchema(IImmutableDictionary<string, object> tables)
        {
            TableMap = tables;
        }
        #endregion

        public DatabaseSchema AddTable<T>(string tableName, TableSchema<T> schema)
        {
            return new DatabaseSchema(TableMap.Add(tableName, schema));
        }

        internal IImmutableDictionary<string, object> TableMap { get; }
    }
}
