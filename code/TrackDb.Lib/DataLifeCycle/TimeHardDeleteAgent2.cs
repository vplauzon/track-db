using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class TimeHardDeleteAgent2 : DataLifeCycleAgentBase
    {
        #region Inner type
        #endregion

        private DateTime _lastRun = DateTime.MinValue;

        public TimeHardDeleteAgent2(Database database)
            : base(database)
        {
        }

        public override void Run(DataManagementActivity forcedDataManagementActivity)
        {
            if (DateTime.Now - _lastRun
                > Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod)
            {
                using (var tx = Database.CreateTransaction())
                {
                    var tombstoneBlockLogic = new TombstoneBlockLogic(Database);
                    var thresholdTime =
                        DateTime.Now - Database.DatabasePolicy.InMemoryPolicy.MaxTombstonePeriod;
                    var tableGroups = GetTombstoneRecordIds(thresholdTime, tx);

                    //  Load tables in tx to eliminate in-memory tombstoned records
                    foreach (var tableGroup in tableGroups)
                    {
                        tx.LoadCommittedBlocksInTransaction(tableGroup.Key);
                    }
                    var allTombstoneBlocksMap = tombstoneBlockLogic.GetTombstoneBlocksMap(
                        tableGroups.Select(g => g.Key),
                        tx);
                    var blockIdsToCompactByTableName = ComputeHardDeletePlan(
                        //  Recompute the tombstones after loading the tables in tx
                        GetTombstoneRecordIds(thresholdTime, tx).ToArray(),
                        allTombstoneBlocksMap,
                        tx);
                    var blockMergingLogic = new BlockMergingLogic3(Database);

                    blockMergingLogic.CompactMerge(
                        blockIdsToCompactByTableName,
                        allTombstoneBlocksMap,
                        tx);

                    tx.Complete();
                }
                _lastRun = DateTime.Now;
            }
        }

        private IEnumerable<IGrouping<string, long>> GetTombstoneRecordIds(
            DateTime thresholdTime,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;

            return Database.TombstoneTable.Query(tx)
                .Where(pf => pf.GreaterThanOrEqual(t => t.Timestamp, thresholdTime))
                .AsEnumerable()
                .GroupBy(t => t.TableName, t => t.DeletedRecordId)
                .Where(g => tableMap[g.Key].IsPersisted);
        }

        private IDictionary<string, IEnumerable<int>> ComputeHardDeletePlan(
            IReadOnlyList<IGrouping<string, long>> tableGroups,
            IDictionary<string, IEnumerable<TombstoneBlock>> tombstoneBlocksMap,
            TransactionContext tx)
        {
            var plan = new Dictionary<string, IEnumerable<int>>(tableGroups.Count);

            foreach (var tableGroup in tableGroups)
            {
                var tableName = tableGroup.Key;

                if (tombstoneBlocksMap.TryGetValue(tableName, out var tombstoneBlocks))
                {
                    var deletedRecordIdSet = tableGroup.ToHashSet();
                    var blockIdsToCompact = new List<int>();

                    foreach (var tombstoneBlock in tombstoneBlocks)
                    {
                        if (deletedRecordIdSet.Overlaps(tombstoneBlock.RecordIds))
                        {
                            blockIdsToCompact.Add(tombstoneBlock.BlockId);
                        }
                    }
                    if (blockIdsToCompact.Count > 0)
                    {
                        plan.Add(tableName, blockIdsToCompact);
                    }
                }
            }

            return plan;
        }
    }
}