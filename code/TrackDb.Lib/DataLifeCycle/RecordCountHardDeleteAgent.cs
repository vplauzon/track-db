using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle.Persistance;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent : DataLifeCycleAgentBase
    {
        private readonly int TOMBSTONE_CLEAN_COUNT = 2000;
        private readonly int META_META_BLOCKS_TOP = 20;

        public RecordCountHardDeleteAgent(Database database)
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
                var metaBlockManager = new MetaBlockManager(Database, tx);

                while (Iteration(doHardDeleteAll, metaBlockManager))
                {
                }

                tx.Complete();
            }
        }

        private bool Iteration(bool doHardDeleteAll, MetaBlockManager metaBlockManager)
        {
            var doIncludeSystemTables = !doHardDeleteAll;
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;

            bool DoNeedCompaction()
            {
                var tombstoneCardinality = Database.TombstoneTable.Query(metaBlockManager.Tx)
                    .WithCommittedOnly()
                    //  We remove the combination of system table under doHardDeleteAll
                    //  As it creates a forever loop with the available-blocks table
                    .Where(t => doIncludeSystemTables || !tableMap[t.TableName].IsSystemTable)
                    .Count();
                var maxTombstonedRecords =
                    Database.DatabasePolicy.InMemoryPolicy.MaxTombstonedRecords;
                var targetHardDeleteCount = doHardDeleteAll
                    ? tombstoneCardinality
                    : tombstoneCardinality - maxTombstonedRecords;

                return targetHardDeleteCount > 0;
            }

            if (DoNeedCompaction())
            {
                HardDeleteRecords(doIncludeSystemTables, DoNeedCompaction, metaBlockManager);

                return true;
            }
            else
            {
                return false;
            }
        }

        private void HardDeleteRecords(
            bool doIncludeSystemTables,
            Func<bool> doNeedCompaction,
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
                    var metaBlockIds = ComputeTopMetaMetaBlocks(metaBlockManager, tableName);

                    foreach (var metaBlockId in metaBlockIds)
                    {
                        if (BlockMergeMetaBlockIds(metaBlockManager, tableName, metaBlockId))
                        {   //  We stop after the first one
                            return;
                        }
                    }
                 
                    //  No tombstone found:  let's clean up phantom tombstones
                    var phantomRowCounts = CleanPhantomTombstones(tableName, metaBlockManager.Tx);

                    Trace.TraceInformation(
                        $"Clean Phantom tombstones ({tableName}):  {phantomRowCounts}");
                }
            }
        }

        private bool BlockMergeMetaBlockIds(
            MetaBlockManager metaBlockManager,
            string tableName,
            int? metaBlockId)
        {
            var blockMergingLogic = new BlockMergingLogic(Database, metaBlockManager);
            var hasMerged = blockMergingLogic.CompactMerge(tableName, metaBlockId);

            return hasMerged;
        }

        private int CleanPhantomTombstones(string tableName, TransactionContext tx)
        {   //  We look for tombstone entries that can't be found in the table
            var deleteRecordIdSet = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Take(TOMBSTONE_CLEAN_COUNT)
                .Select(t => t.DeletedRecordId)
                .ToHashSet();
            var table = Database.GetAnyTable(tableName);
            var foundRecordIds = table.Query(tx)
                .WithIgnoreDeleted()
                .WithProjection(table.Schema.RecordIdColumnIndex)
                .WithPredicate(new InPredicate(
                    table.Schema.RecordIdColumnIndex,
                    deleteRecordIdSet.Cast<object?>(),
                    true))
                .Select(r => (long)r.Span[0]!)
                .ToImmutableArray();
            var phantomRecordIds = deleteRecordIdSet
                .Except(foundRecordIds)
                .ToImmutableArray();

            if (phantomRecordIds.Length == 0)
            {
                throw new InvalidOperationException(
                    $"Phantom records in table '{tableName}' but we can't find any");
            }

            //  We then hard delete those records
            tx.LoadCommittedBlocksInTransaction(Database.TombstoneTable.Schema.TableName);

            var block = tx.TransactionState
                .UncommittedTransactionLog
                .TransactionTableLogMap[Database.TombstoneTable.Schema.TableName]
                .CommittedDataBlock!;
            var predicate = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.In(t => t.DeletedRecordId, phantomRecordIds))
                .Predicate;
            var rowIndexes = ((IBlock)block).Filter(predicate, false)
                .RowIndexes;

            block.DeleteRecordsByRecordIndex(rowIndexes);

            return rowIndexes.Count();
        }

        private IEnumerable<int?> ComputeTopMetaMetaBlocks(
            MetaBlockManager metaBlockManager,
            string tableName)
        {
            var metaMetaBlockStats = metaBlockManager.ListMetaMetaBlocks(tableName);
            var queue = new PriorityQueue<MetaMetaBlockStat, long>(META_META_BLOCKS_TOP);

            //  Scan meta meta blocks and keep the top META_META_BLOCKS_TOP (by count)
            foreach (var stats in metaMetaBlockStats)
            {
                if (queue.Count < META_META_BLOCKS_TOP
                    || queue.Peek().TombstonedRecordCount < stats.TombstonedRecordCount)
                {
                    if (queue.Count == META_META_BLOCKS_TOP)
                    {
                        queue.Dequeue();
                    }
                    queue.Enqueue(stats, stats.TombstonedRecordCount);
                }
            }

            return queue.UnorderedItems
                .AsEnumerable()
                .Select(p => p.Element)
                .OrderByDescending(s => s.TombstonedRecordCount)
                .Select(s => s.BlockId)
                .ToImmutableList();
        }
    }
}