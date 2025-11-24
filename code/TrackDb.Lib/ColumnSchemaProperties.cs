using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal readonly record struct ColumnSchemaProperties(
        ColumnSchema ColumnSchema,
        ColumnSchemaStat ColumnSchemaStat,
        ColumnSchema? ParentColumnSchema = null);
}