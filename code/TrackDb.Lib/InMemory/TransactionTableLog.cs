using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using TrackDb.Lib.InMemory.Block;

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

        #region Debug View
        /// <summary>To be used in debugging only.</summary>
        internal DataTable DebugView
        {
            get
            {
                var dataTables = new List<DataTable>();

                dataTables.Add(NewDataBlock.DebugView);
                if (CommittedDataBlock != null)
                {
                    dataTables.Add(CommittedDataBlock.DebugView);
                }

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
        #endregion

    }
}