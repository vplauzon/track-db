using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib
{
    internal class MetadataTableSchema : TableSchema
    {
        public MetadataTableSchema(
            string tableName,
            IImmutableList<ColumnSchema> columns)
            : base(tableName, columns, ImmutableArray<int>.Empty, ImmutableArray<int>.Empty)
        {
        }
    }
}