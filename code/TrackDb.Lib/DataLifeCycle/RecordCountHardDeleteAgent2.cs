using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent2 : DataLifeCycleAgentBase
    {
        #region Inner type
        private record DeletedRecordsByBlock(BlockTrace[] BlockTraces, long[] DeletedRecordIds);
        #endregion

        public RecordCountHardDeleteAgent2(Database database)
            : base(database)
        {
        }

        public override void Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doHardDeleteAll =
                (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll)
                == DataManagementActivity.HardDeleteAll;

            using (var tx = Database.CreateTransaction())
            {
                if (IsHardDeleteRequiredAfterInMemoryCompact(tx) || doHardDeleteAll)
                {
                    var map = ComputeHardDeletePlan(doHardDeleteAll, tx);
                }

                tx.Complete();
            }
        }

        private IDictionary<string, DeletedRecordsByBlock[]> ComputeHardDeletePlan(
            bool doHardDeleteAll,
            TransactionContext tx)
        {
            var recordCountDelta = doHardDeleteAll
                ? Database.TombstoneTable.Query(tx).Count()
                : Database.TombstoneTable.Query(tx).Count() -
                Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;
            var tableBlockMap = GetTableBlockMap(tx);
            var tableBlocks = tableBlockMap
                .SelectMany(p => p.Value.Select(drb => new
                {
                    TableName = p.Key,
                    DeletedRecordsByBlock = drb
                }))
                .OrderByDescending(o => o.DeletedRecordsByBlock.DeletedRecordIds.Length)
                .ToList();
            var currentTableBlockCount = 0;
            var currentRecordCountDelta = 0;

            foreach (var tableBlock in tableBlocks)
            {
                if (currentRecordCountDelta >= recordCountDelta)
                {
                    break;
                }
                currentRecordCountDelta += tableBlock.DeletedRecordsByBlock.DeletedRecordIds.Length;
                ++currentTableBlockCount;
            }

            var deltaTableBlockMap = tableBlocks
                .Take(currentTableBlockCount)
                .GroupBy(o => o.TableName)
                .ToDictionary(g => g.Key, g => g.Select(o => o.DeletedRecordsByBlock).ToArray());

            return deltaTableBlockMap;
        }

        private Dictionary<string, DeletedRecordsByBlock[]> GetTableBlockMap(TransactionContext tx)
        {
            var tombstoneSchema = Database.TombstoneTable.Schema;
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var columnIndexes = tombstoneSchema.GetColumnIndexSubset(t => t.TableName)
                .Concat(tombstoneSchema.GetColumnIndexSubset(t => t.DeletedRecordId));
            var tableTombstoneMap = Database.TombstoneTable.Query(tx)
                .TableQuery
                .WithProjection(columnIndexes)
                .Select(r => new
                {
                    TableName = (string)r.Span[0]!,
                    DeletedRecordId = (long)r.Span[1]!
                })
                .GroupBy(o => o.TableName)
                .Where(g => tableMap[g.Key].IsPersisted)
                .ToDictionary(g => g.Key, g => g.Select(o => o.DeletedRecordId).ToHashSet());
            var tableBlockMap = tableTombstoneMap.Keys
                .Select(tableName => new
                {
                    TableName = tableName,
                    DeletedRecordsByBlock = GetDeletedRecordsByBlock(
                        tableName,
                        tableTombstoneMap[tableName],
                        tx)
                })
                .ToDictionary(o => o.TableName, o => o.DeletedRecordsByBlock);

            return tableBlockMap;
        }

        private DeletedRecordsByBlock[] GetDeletedRecordsByBlock(
            string tableName,
            ISet<long> recordIds,
            TransactionContext tx)
        {
            var table = Database.GetAnyTable(tableName);
            var predicate = new InPredicate<long>(
                table.Schema.RecordIdColumnIndex,
                recordIds,
                false,
                true);
            var blocks = table.Query(tx)
                .WithIgnoreDeleted()
                .WithPredicate(predicate)
                .WithProjection(table.Schema.RecordIdColumnIndex)
                .ExecuteQueryWithBlockTrace()
                .Select(btr => new
                {
                    LastBlockId = btr.BlockTraces.Last().BlockId,
                    //  Copy traces
                    BlockTraces = btr.BlockTraces.ToArray(),
                    DeletedRecordId = (long)btr.Result.Span[0]!
                })
                .GroupBy(o => o.LastBlockId)
                .Select(g => new DeletedRecordsByBlock(
                    g.First().BlockTraces,
                    g.Select(o => o.DeletedRecordId).ToArray()))
                .ToArray();

            return blocks;
        }

        private bool IsHardDeleteRequiredAfterInMemoryCompact(TransactionContext tx)
        {
            bool IsAboveThreshold() => Database.TombstoneTable.Query(tx).Count()
                > Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;

            if (!IsAboveThreshold())
            {
                return false;
            }
            else
            {
                var tombstoneSchema = Database.TombstoneTable.Schema;
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
                var tableCounts = Database.TombstoneTable.Query(tx)
                    .TableQuery
                    .WithProjection(tombstoneSchema.GetColumnIndexSubset(t => t.TableName))
                    .Select(r => (string)r.Span[0]!)
                    .CountBy(name => name)
                    .Select(o => new
                    {
                        TableName = o.Key,
                        tableMap[o.Key].IsPersisted,
                        RowCount = o.Value
                    })
                    //  Put unpersisted table first so they get compacted first
                    .OrderBy(o => o.IsPersisted ? 1 : 0)
                    .ThenBy(o => o.RowCount)
                    .Select(o => o.TableName)
                    .ToList();

                do
                {
                    var tableToCompact = tableCounts.Last();
                    var hasLoaded = tx.LoadCommittedBlocksInTransaction(tableToCompact);

                    tableCounts.RemoveAt(tableCounts.Count - 1);
                    if (!IsAboveThreshold())
                    {
                        return false;
                    }
                }
                while (tableCounts.Count > 0);

                return true;
            }
        }
    }
}