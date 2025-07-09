using System.Collections.Generic;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal interface IBlock
    {
        TableSchema TableSchema { get; }

        int RecordCount { get; }

        IEnumerable<long> RecordIds { get; }
        
        IEnumerable<object> GetColumnData(int columnIndex);
    }
}