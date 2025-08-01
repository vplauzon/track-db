using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal interface IBlock
    {
        TableSchema TableSchema { get; }

        int RecordCount { get; }

        IEnumerable<long> RecordIds { get; }
        
        IEnumerable<object?> GetColumnData(int columnIndex);

        /// <summary>Performs a query within the block's data.</summary>
        /// <param name="predicate"></param>
        /// <param name="projectionColumnIndexes"></param>
        /// <returns>Projected columns of rows matching query.</returns>
        IEnumerable<ReadOnlyMemory<object?>> Query(
            IQueryPredicate predicate,
            IEnumerable<int> projectionColumnIndexes);
    }
}