using System;

namespace TrackDb.Lib.Logging
{
    internal record ColumnSchemaContent(string ColumnName, string ColumnType)
    {
        public ColumnSchemaContent(ColumnSchema schema)
            : this(schema.ColumnName, schema.ColumnType.Name)
        {
        }
    }
}