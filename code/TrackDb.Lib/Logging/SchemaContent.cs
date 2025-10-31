using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace TrackDb.Lib.Logging
{
    internal record SchemaContent(
        IImmutableList<TableSchemaContent> Tables)
        : ContentBase<SchemaContent>
    {
        public static SchemaContent FromSchemas(IEnumerable<TableSchema> schemas)
        {
            var tables = schemas
                .Select(s => new TableSchemaContent(
                    s.TableName,
                    s.Columns
                    .Select(c => new ColumnSchemaContent(c.ColumnName, c.ColumnType.Name))
                    .ToImmutableArray()))
                .ToImmutableArray();

            return new(tables);
        }
    }
}