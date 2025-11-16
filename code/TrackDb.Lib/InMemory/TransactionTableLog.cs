using TrackDb.Lib.InMemory.Block;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory
{
    internal record TransactionTableLog(
        BlockBuilder NewDataBlock,
        BlockBuilder? CommittedDataBlock = null)
    {
        public TransactionTableLog(TableSchema schema, BlockBuilder? CommittedDataBlock = null)
            : this(new BlockBuilder(schema), CommittedDataBlock)
        {
        }
    }
}