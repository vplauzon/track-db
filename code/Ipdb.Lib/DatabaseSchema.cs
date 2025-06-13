using System;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib
{
    public class DatabaseSchema
    {
        #region Constructor
        public DatabaseSchema()
            : this(ImmutableArray<TableSchema>.Empty)
        {
        }

        private DatabaseSchema(IImmutableList<TableSchema> tableSchemas)
        {
            TableSchemas = tableSchemas;
        }
        #endregion

        public DatabaseSchema AddTable<T>(TableSchema<T> schema)
        {
            return new DatabaseSchema(TableSchemas.Add(schema));
        }

        internal IImmutableList<TableSchema> TableSchemas { get; }
    }
}
