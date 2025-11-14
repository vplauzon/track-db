using TrackDb.Lib.InMemory.Block;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory
{
    internal record TransactionTableLog(
        BlockBuilder NewDataBlockBuilder,
        IBlock? CommittedDataBlock = null)
    {
        public TransactionTableLog(TableSchema schema)
            : this(new BlockBuilder(schema))
        {
        }
    }
}