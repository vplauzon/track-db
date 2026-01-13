using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.DataLifeCycle
{
    internal class RecordCountHardDeleteAgent : DataLifeCycleAgentBase
    {
        #region Inner Types
        private record MinMaxRecordId(long Min, long Max);

        private record TombstoneBlock(int? BlockId, long TombstoneCount);

        private class TopTombstoneBlocks
        {
            private readonly int _capacity;
            private readonly PriorityQueue<TombstoneBlock, long> _blocks = new();

            public TopTombstoneBlocks(int capacity)
            {
                _capacity = capacity;
            }

            public void Add(TombstoneBlock block)
            {
                if (_blocks.Count == _capacity
                    && _blocks.Peek().TombstoneCount < block.TombstoneCount)
                {   //  We let the lowest count go
                    _blocks.Dequeue();
                }
                if (_blocks.Count < _capacity)
                {
                    _blocks.Enqueue(block, block.TombstoneCount);
                }
            }

            public IEnumerable<int?> TopTombstoneBlockIds =>
                _blocks.UnorderedItems
                .Select(t => t.Element)
                .OrderByDescending(t => t.TombstoneCount)
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

            while (Iteration(doHardDeleteAll, maxTombstonedRecords))
            {
            }
        }

        private bool Iteration(bool doHardDeleteAll, int maxTombstonedRecords)
        {
            using (var tx = Database.CreateTransaction())
            {
                var result = TransactionalIteration(doHardDeleteAll, maxTombstonedRecords, tx);

                tx.Complete();

                return result;
            }
        }

        private bool TransactionalIteration(
            bool doHardDeleteAll,
            int maxTombstonedRecords,
            TransactionContext tx)
        {
            var doIncludeSystemTables = !doHardDeleteAll;
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var tombstoneCardinality = Database.TombstoneTable.Query(tx)
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
                HardDeleteRecords(doIncludeSystemTables, targetHardDeleteCount, tx);
                //CleanOneBlock(doIncludeSystemTables, tx);

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
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            //  Take the table with the most tombstones
            var topTable = Database.TombstoneTable.Query(tx)
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

                if (!tx.LoadCommittedBlocksInTransaction(tableName))
                {
                    var metaBlockIds = new Stack<int?>(ComputeOptimalMetaBlocks(tableName, tx));
                    var blockMergingLogic = new BlockMergingLogic2(Database);

                    while (targetHardDeleteCount > 0 && metaBlockIds.Count() > 0)
                    {
                        var metaBlockId = metaBlockIds.Pop();
                        var hardDeletedCount =
                            blockMergingLogic.CompactMerge(tableName, metaBlockId, tx);

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
        /// <param name="tx"></param>
        /// <returns></returns>
        private IEnumerable<int?> ComputeOptimalMetaBlocks(
            string tableName,
            TransactionContext tx)
        {
            var metaMetaDataTable = GetMetaMetaDataTable(tableName);

            if (metaMetaDataTable == null)
            {
                return [null];
            }
            else
            {
                var tombstoneExtrema = GetTombstoneRecordIdExtrema(tableName, tx);
                var metaMetaSchema = (MetadataTableSchema)metaMetaDataTable.Schema;
                var metaMetaRecords = metaMetaDataTable.Query(tx)
                    .WithCommittedOnly()
                    .WithProjection(
                    metaMetaSchema.BlockIdColumnIndex,
                    metaMetaSchema.RecordIdMinColumnIndex,
                    metaMetaSchema.RecordIdMaxColumnIndex)
                    .Select(r => new
                    {
                        BlockId = (int?)r.Span[0],
                        MinRecordId = (long)r.Span[1]!,
                        MaxRecordId = (long)r.Span[2]!
                    });
                var tombstoneBlocks = new TopTombstoneBlocks(META_BLOCK_COUNT);

                foreach (var m in metaMetaRecords)
                {   //  Check if the meta block might have tombstone records
                    //  This is to avoid doing a tombstone query for each meta block
                    //  We test for intersection
                    if (tombstoneExtrema.Max >= m.MinRecordId
                        && m.MaxRecordId >= tombstoneExtrema.Min)
                    {
                        var recordCount = Database.TombstoneTable.Query(tx)
                            .WithCommittedOnly()
                            .Where(pf => pf.Equal(t => t.TableName, tableName))
                            .Where(pf => pf.GreaterThanOrEqual(t => t.DeletedRecordId, m.MinRecordId)
                            .And(pf.LessThanOrEqual(t => t.DeletedRecordId, m.MaxRecordId)))
                            .Count();
                        var tombstoneBlock = new TombstoneBlock(m.BlockId, recordCount);

                        tombstoneBlocks.Add(tombstoneBlock);
                    }
                }

                return tombstoneBlocks.TopTombstoneBlockIds;
            }
        }

        private MinMaxRecordId GetTombstoneRecordIdExtrema(string tableName, TransactionContext tx)
        {
            var query = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .TableQuery
                .WithProjection(Database.TombstoneTable.Schema.GetColumnIndexSubset(
                    t => t.DeletedRecordId))
                .Select(r => (long)r.Span[0]!);
            var aggregate = query
                .Aggregate(new MinMaxRecordId(0, 0), (aggregate, recordId) => new MinMaxRecordId(
                    Math.Min(aggregate.Min, recordId),
                    Math.Max(aggregate.Max, recordId)));

            return aggregate;
        }

        private Table? GetMetaMetaDataTable(string tableName)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var metaDataTableName = tableMap[tableName].MetaDataTableName;

            if (metaDataTableName == null)
            {
                throw new InvalidOperationException(
                    $"Table {tableName} doesn't have an associated metadata table when " +
                    $"hard delete occure");
            }

            var metaMetaDataTableName = tableMap[metaDataTableName].MetaDataTableName;

            return metaMetaDataTableName == null
                ? null
                : tableMap[metaMetaDataTableName].Table;
        }

        private void CleanOneBlock(bool doIncludeSystemTables, TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var topBlocks = Database.TombstoneTable.Query(tx)
                .WithCommittedOnly()
                .Where(t => doIncludeSystemTables || !tableMap[t.TableName].IsSystemTable)
                //  Count records per block
                .CountBy(t => (t.TableName, t.BlockId ?? 0))
                .Select(p => new
                {
                    p.Key.TableName,
                    BlockId = p.Key.Item2 <= 0 ? null : (int?)p.Key.Item2,
                    RecordCount = p.Value
                })
                .OrderByDescending(o => o.RecordCount)
                //  Cap the collection to required item count
                .Take(2 * Database.DatabasePolicy.InMemoryPolicy.MaxNonMetaDataRecords);
            var tableName = topBlocks.First().TableName;
            var hasNullBlocks = topBlocks
                .Where(o => o.TableName == tableName)
                .Where(o => o.BlockId == null)
                .Any();

            if (tableMap[tableName].IsMetaDataTable)
            {
                throw new InvalidOperationException(
                    $"A metadata table ({tableName}) has tombstone entries");
            }
            if (hasNullBlocks)
            {
                var tombstoneBlockFixLogic = new TombstoneBlockFixLogic(Database);

                tombstoneBlockFixLogic.FixNullBlockIds(tableName, tx);
            }
            else
            {
                var blockId = topBlocks.First().BlockId!.Value;
                var otherBlockIds = topBlocks
                    .Skip(1)
                    .Where(o => o.TableName == tableName)
                    .Select(o => o.BlockId!.Value)
                    .ToImmutableArray();

                CompactBlock(tableName, blockId, otherBlockIds, tx);
            }
        }

        private void CompactBlock(
            string tableName,
            int blockId,
            IEnumerable<int> otherBlockIds,
            TransactionContext tx)
        {
            var blockMergingLogic = new BlockMergingLogic(Database);

            if (!blockMergingLogic.CompactBlock(tableName, blockId, otherBlockIds, tx))
            {
                var tombstoneBlockFixLogic = new TombstoneBlockFixLogic(Database);

                tombstoneBlockFixLogic.FixBlockId(tableName, blockId, tx);
            }
        }
    }
}