using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal record ColumnSchemaProperties(
        ColumnSchema ColumnSchema,
        ColumnSchemaStat ColumnSchemaStat,
        ColumnSchemaProperties? ParentColumnProperties = null)
    {
        public ColumnSchemaProperties GetAncestorZero()
        {
            if (ParentColumnProperties == null)
            {
                return this;
            }
            else
            {
                return ParentColumnProperties.GetAncestorZero();
            }
        }
    }
}