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

        IEnumerable<object> GetRecords(IEnumerable<long> recordIds);

        /// <summary>Performs the query within the block's data.</summary>
        /// <param name="predicate"></param>
        /// <returns>Returns record IDs matching the query.</returns>
        IEnumerable<long> Query(IQueryPredicate predicate);
    }
}