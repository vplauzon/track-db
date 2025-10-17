using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Logging
{
    internal record SchemaContent(
        IImmutableDictionary<string, IImmutableList<ColumnSchemaContent>> Tables)
    {
        public SchemaContent(IEnumerable<TableSchema> schemas)
            : this(FromTableSchemas(schemas))
        {
        }

        private static IImmutableDictionary<string, IImmutableList<ColumnSchemaContent>> FromTableSchemas(
            IEnumerable<TableSchema> schemas)
        {
            return schemas
                .Select(s => new
                {
                    s.TableName,
                    ColumnSchemas = s.Columns
                    .Select(c => new ColumnSchemaContent(c))
                    .ToImmutableArray()
                })
                .ToImmutableDictionary(
                o => o.TableName,
                o => (IImmutableList<ColumnSchemaContent>)o.ColumnSchemas);
        }
    }
}