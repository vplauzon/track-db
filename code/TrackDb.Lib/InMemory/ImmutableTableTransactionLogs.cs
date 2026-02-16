using System;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Threading;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.InMemory
{
    internal record ImmutableTableTransactionLogs(IImmutableList<IBlock> InMemoryBlocks)
    {
        public ImmutableTableTransactionLogs()
            : this(ImmutableArray<IBlock>.Empty)
        {
        }

        public ImmutableTableTransactionLogs(IBlock block)
            : this(new[] { block }.ToImmutableArray())
        {
        }

        #region Debug View
        /// <summary>To be used in debugging only.</summary>
        internal DataTable DebugView
        {
            get
            {
                if (InMemoryBlocks.Count == 0)
                {
                    return new DataTable();
                }
                else
                {
                    var dataTables = InMemoryBlocks
                        .Select(b => (BlockBuilder)b)
                        .Select(b => b.DebugView)
                        .ToImmutableArray();
                    var mergedTable = dataTables[0].Clone();
                    var rows = dataTables
                        .SelectMany(t => t.Rows.Cast<DataRow>());

                    foreach (var row in rows)
                    {
                        mergedTable.ImportRow(row);
                    }

                    return mergedTable;
                }
            }
        }
        #endregion

        public BlockBuilder MergeLogs()
        {
            if (!InMemoryBlocks.Any())
            {
                throw new InvalidOperationException("No in memory blocks to merge");
            }

            var newBlock = new BlockBuilder(InMemoryBlocks.First().TableSchema);

            foreach (var block in InMemoryBlocks)
            {
                newBlock.AppendBlock(block);
            }

            return newBlock;
        }
    }
}