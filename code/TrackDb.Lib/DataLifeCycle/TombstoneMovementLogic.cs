using System;
using System.Collections.Generic;
using System.Linq;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    /// <summary>
    /// Move tombstone records from the tombstone table to <see cref="BlockTombstones"/>.
    /// </summary>
    internal class TombstoneMovementLogic : LogicBase
    {
        public TombstoneMovementLogic(Database database)
            : base(database)
        {
        }

        public void MoveTombstones(TransactionContext tx)
        {
            var recordIdsbyTable = Database.TombstoneTable.Query(tx)
                .GroupBy(t => t.TableName)
                .ToArray();

            if (recordIdsbyTable.Length > 0)
            {
                tx.LoadBlockTombstonesInTransaction();
                
                var blockTombstonesIndex =
                    tx.TransactionState.UncommittedTransactionLog.ReplacingBlockTombstonesIndex!;

                foreach (var group in recordIdsbyTable)
                {
                    var recordIds = group
                        .Select(t => t.DeletedRecordId);

                    MoveTableTombstones(group.Key, recordIds, blockTombstonesIndex, tx);
                }
                tx.CleanTable(Database.TombstoneTable.Schema);
            }
        }

        private void MoveTableTombstones(
            string tableName,
            IEnumerable<long> recordIds,
            IDictionary<int, BlockTombstones> blockTombstonesIndex,
            TransactionContext tx)
        {
            var table = Database.GetAnyTable(tableName);
            var recordIdColumnIndex = ((DataTableSchema)table.Schema).RecordIdColumnIndex;
            var inPredicate = new InPredicate<long>(recordIdColumnIndex, recordIds, true);
            var resultByBlockIds = table.Query(tx)
                .WithPredicate(inPredicate)
                .WithProjection(recordIdColumnIndex)
                .WithIgnoreDeleted()
                .ExecuteQueryWithBlockTrace()
                .Select(r => new
                {
                    BlockTrace = r.BlockTraces[r.BlockTraces.Count - 1],
                    RecordId = (long)r.Result.Span[0]!
                })
                .Select(o => new
                {
                    BlockId = o.BlockTrace.BlockId >= 1 ? o.BlockTrace.BlockId : (int?)null,
                    o.BlockTrace.RecordCountInBlock,
                    o.BlockTrace.RowIndex,
                    o.RecordId
                })
                .GroupBy(o => o.BlockId);

            foreach (var group in resultByBlockIds)
            {
                if (group.Key == null)
                {
                    tx.LoadCommittedBlocksInTransaction(tableName);
                }
                else
                {
                    MoveBlockTombstones(
                        tableName,
                        group.Key.Value,
                        group.Select(o => o.RecordCountInBlock).First(),
                        group.Select(o => o.RecordId),
                        group.Select(o => o.RowIndex),
                        blockTombstonesIndex,
                        tx);
                }
            }
        }

        private void MoveBlockTombstones(
            string tableName,
            int blockId,
            int itemCount,
            IEnumerable<long> recordIds,
            IEnumerable<int> rowIndexes,
            IDictionary<int, BlockTombstones> blockTombstonesIndex,
            TransactionContext tx)
        {
            if (blockTombstonesIndex.TryGetValue(blockId, out var blockTombstones))
            {
                blockTombstonesIndex[blockId] = blockTombstones.AddRowIndexes(rowIndexes);
            }
            else
            {
                blockTombstonesIndex[blockId] = new BlockTombstones(
                    blockId,
                    tableName,
                    itemCount,
                    rowIndexes);
            }
        }
    }
}