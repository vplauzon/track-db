using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent2 : DataLifeCycleAgentBase
    {
        #region Inner type
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
                    var tombstoneBlockLogic = new TombstoneBlockLogic(Database);
                    var tombstoneBlocksMap = tombstoneBlockLogic.GetTombstoneBlocksMap(null, tx);
                    var plan = ComputeHardDeletePlan(tombstoneBlocksMap, doHardDeleteAll, tx);
                    var blockMergingLogic = new BlockMergingLogic3(Database);

                    blockMergingLogic.CompactMerge(plan, tombstoneBlocksMap, tx);
                }

                tx.Complete();
            }
        }

        private IDictionary<string, IEnumerable<TombstoneBlock>> ComputeHardDeletePlan(
            IDictionary<string, IEnumerable<TombstoneBlock>> tombstoneBlocksMap,
            bool doHardDeleteAll,
            TransactionContext tx)
        {
            var recordCountDelta = doHardDeleteAll
                ? Database.TombstoneTable.Query(tx).Count()
                : Database.TombstoneTable.Query(tx).Count() -
                Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;
            var tableBlocks = tombstoneBlocksMap
                .SelectMany(p => p.Value.Select(tb => new
                {
                    TableName = p.Key,
                    TombstoneBlock = tb
                }))
                .OrderByDescending(o => o.TombstoneBlock.RecordIds.Count)
                .ToList();
            var currentTableBlockCount = 0;
            var currentRecordCountDelta = 0;

            foreach (var tableBlock in tableBlocks)
            {
                if (currentRecordCountDelta >= recordCountDelta)
                {
                    break;
                }
                currentRecordCountDelta += tableBlock.TombstoneBlock.RecordIds.Count;
                ++currentTableBlockCount;
            }

            var plan = tableBlocks
                .Take(currentTableBlockCount)
                .GroupBy(o => o.TableName)
                .ToDictionary(g => g.Key, g => g.Select(o => o.TombstoneBlock).ToArray().AsEnumerable());

            return plan;
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