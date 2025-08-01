using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal abstract class ReadOnlyBlockBase
    {
        protected ReadOnlyBlockBase(
            TableSchema schema,
            IEnumerable<IReadOnlyDataColumn> dataColumns)
        {
            Schema = schema;
            DataColumns = dataColumns.ToImmutableArray();
        }

        protected TableSchema Schema { get; }

        protected IImmutableList<IReadOnlyDataColumn> DataColumns { get; }
    }
}