using System;
using System.Collections.Generic;
using System.Text;

namespace TrackDb.Lib.DataLifeCycle
{
    internal record TombstoneBlock(
        IReadOnlyList<BlockTrace> BlockTraces,
        //  Schema of the block's table
        TableSchema Schema,
        //  Block owning the record ids
        int BlockId,
        //  Row indexes of deleted records
        IReadOnlyList<int> RowIndexes,
        //  Record IDs of deleted records
        IReadOnlyList<long> RecordIds);
}
