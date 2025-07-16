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
        
        IEnumerable<object> GetColumnData(int columnIndex);

        IImmutableList<object> GetRecords(IEnumerable<long> recordIds);
        
        IImmutableList<long> Query(IQueryPredicate predicate, int? takeCount);
    }
}