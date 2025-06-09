using System;
using System.Collections.Immutable;

namespace Ipdb.Lib
{
    public class DatabaseSchema
    {
        private readonly IImmutableList<(string Name, object Schema)> _tables;

        #region Constructor
        public DatabaseSchema()
            : this(ImmutableArray<(string Name, object Schema)>.Empty)
        {
        }

        private DatabaseSchema(IImmutableList<(string Name, object Schema)> tables)
        {
            _tables = tables;
        }
        #endregion

        public DatabaseSchema AddTable<T>(string tableName, TableSchema<T> schema)
        {
            return new DatabaseSchema(_tables.Add((tableName, schema)));
        }
    }
}
