using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle.Persistance;
using static TrackDb.Lib.DataLifeCycle.Persistance.MetaBlockManager;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent : DataLifeCycleAgentBase
    {
        #region Inner Types
        private class TopTombstoneBlocks
        {
            private readonly int _capacity;
            private readonly PriorityQueue<MetaMetaBlockStat, long> _blocks = new();

            public TopTombstoneBlocks(int capacity)
            {
                _capacity = capacity;
            }

            public void Add(MetaMetaBlockStat block)
            {
                if (_blocks.Count == _capacity
                    && _blocks.Peek().TombstonedRecordCount < block.TombstonedRecordCount)
                {   //  We let the lowest count go
                    _blocks.Dequeue();
                }
                if (_blocks.Count < _capacity)
                {
                    _blocks.Enqueue(block, block.TombstonedRecordCount);
                }
            }

            public IEnumerable<int?> TopTombstoneBlockIds =>
                _blocks.UnorderedItems
                .Select(t => t.Element)
                .OrderByDescending(t => t.TombstonedRecordCount)
                .Select(t => t.BlockId);
        }
        #endregion

        private readonly int META_BLOCK_COUNT = 10;

        public RecordCountHardDeleteAgent(Database database)
            : base(database)
        {
        }

        public override void Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doHardDeleteAll =
                (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll)
                == DataManagementActivity.HardDeleteAll;
            var maxTombstonedRecords = Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;

            using (var tx = Database.CreateTransaction())
            {
                var metaBlockManager = new MetaBlockManager(Database, tx);

                while (Iteration(doHardDeleteAll, maxTombstonedRecords, metaBlockManager))
                {
                }

                tx.Complete();
            }
        }

        private bool Iteration(
            bool doHardDeleteAll,
            int maxTombstonedRecords,
            MetaBlockManager metaBlockManager)
        {
            var doIncludeSystemTables = !doHardDeleteAll;
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tombstoneCardinality = Database.TombstoneTable.Query(metaBlockManager.Tx)
                .WithCommittedOnly()
                //  We remove the combination of system table under doHardDeleteAll
                //  As it creates a forever loop with the available-blocks table
                .Where(t => doIncludeSystemTables || !tableMap[t.TableName].IsSystemTable)
                .Count();
            var targetHardDeleteCount = doHardDeleteAll
                ? tombstoneCardinality
                : tombstoneCardinality - maxTombstonedRecords;

            if (targetHardDeleteCount > 0)
            {
                HardDeleteRecords(doIncludeSystemTables, targetHardDeleteCount, metaBlockManager);

                return true;
            }
            else
            {
                return false;
            }
        }

        private void HardDeleteRecords(
            bool doIncludeSystemTables,
            int targetHardDeleteCount,
            MetaBlockManager metaBlockManager)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            //  Take the table with the most tombstones
            var topTable = Database.TombstoneTable.Query(metaBlockManager.Tx)
                .WithCommittedOnly()
                .Where(t => doIncludeSystemTables || !tableMap[t.TableName].IsSystemTable)
                .CountBy(t => t.TableName)
                .Select(p => new
                {
                    TableName = p.Key,
                    RecordCount = p.Value
                })
                .OrderByDescending(o => o.RecordCount)
                .FirstOrDefault();

            if (topTable == null)
            {
                throw new InvalidOperationException(
                    "There should be at least one table with tombtoned records");
            }
            else
            {
                var tableName = topTable.TableName;

                //  If we see changes by loading data, we skip hard delete and let recompute top table
                if (!metaBlockManager.Tx.LoadCommittedBlocksInTransaction(tableName))
                {
                    var metaBlockIds = new Stack<int?>(
                        ComputeOptimalMetaBlocks(metaBlockManager, tableName));
                    var blockMergingLogic = new BlockMergingLogic2(Database, metaBlockManager);

                    while (targetHardDeleteCount > 0 && metaBlockIds.Count() > 0)
                    {
                        var metaBlockId = metaBlockIds.Pop();
                        var hardDeletedCount = blockMergingLogic.CompactMerge(tableName, metaBlockId);

                        targetHardDeleteCount -= hardDeletedCount;
                    }
                }
            }
        }

        /// <summary>
        /// We return multiple meta block IDs.  This allows us to leverage one scan but mostly it's
        /// in case tombstone records aren't in the meta block (phantom tombstone records or overlapping
        /// meta blocks).
        /// 
        /// So we return the top-N optimal meta blocks.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="metaBlockManager"></param>
        /// <returns></returns>
        private IEnumerable<int?> ComputeOptimalMetaBlocks(
            MetaBlockManager metaBlockManager,
            string tableName)
        {
            var tombstoneBlocks = new TopTombstoneBlocks(META_BLOCK_COUNT);
            var metaMetaBlockStats = metaBlockManager.ListMetaMetaBlocks(tableName);

            foreach (var mb in metaMetaBlockStats)
            {
                tombstoneBlocks.Add(mb);
            }

            return tombstoneBlocks.TopTombstoneBlockIds;
        }
    }
}