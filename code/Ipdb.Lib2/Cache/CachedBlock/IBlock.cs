using Ipdb.Lib2.Query;
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
        /// <returns>Returns results matching the query.</returns>
        IEnumerable<QueryResult> Query(
            IQueryPredicate predicate,
            IImmutableList<int> projectionColumnIndexes);
    }
}