using System;
using System.Collections.Immutable;

namespace Ipdb.Lib
{
    public class DatabaseSchema
    {
        private readonly IImmutableDictionary<string, object> _tables;

        #region Constructor
        public DatabaseSchema()
            : this(ImmutableDictionary<string, object>.Empty)
        {
        }

        private DatabaseSchema(IImmutableDictionary<string, object> tables)
        {
            _tables = tables;
        }
        #endregion

        public DatabaseSchema AddTable<T>(string tableName, TableSchema<T> schema)
        {
            return new DatabaseSchema(_tables.Add(tableName, schema));
        }
    }
}
