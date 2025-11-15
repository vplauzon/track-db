using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class DataLifeCycleAgentBase
    {
        private readonly Lazy<DatabaseFileManager> _storageManager;

        protected DataLifeCycleAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
        {
            Database = database;
            TombstoneTable = tombstoneTable;
            _storageManager = storageManager;
        }

        /// <summary>Runs an agent logic.</summary>
        /// <param name="forcedDataManagementActivity"></param>
        /// <returns><c>true</c> iif the agent has run to completion and we can go to the next agent.</returns>
        public abstract bool Run(DataManagementActivity forcedDataManagementActivity);

        protected Database Database { get; }

        protected TypedTable<TombstoneRecord> TombstoneTable { get; }

        protected DatabaseFileManager StorageManager => _storageManager.Value;

        protected bool MergeTableTransactionLogs(string tableName)
        {
            using (var tx = Database.CreateDummyTransaction())
            {
                var map = tx.TransactionState.InMemoryDatabase.TransactionTableLogsMap;

                if (map.ContainsKey(tableName))
                {
                    var logs = map[tableName];

                    if (logs.InMemoryBlocks.Count == 1
                        && !AreTombstoneRecords(logs.InMemoryBlocks.First(), tx))
                    {   //  Nothing to merge
                        return false;
                    }
                    var blockBuilder = logs.MergeLogs();

                    if (tableName != TombstoneTable.Schema.TableName
                        && map.ContainsKey(TombstoneTable.Schema.TableName))
                    {
                        var tombstoneLogs = map[TombstoneTable.Schema.TableName];
                        var tombstoneBuilder = tombstoneLogs.MergeLogs();

                        TrimBlocks(blockBuilder, tombstoneBuilder);
                        CommitMerge(blockBuilder, tombstoneBuilder, tx);
                    }
                    else
                    {
                        CommitMerge(blockBuilder, null, tx);
                    }

                    return true;
                }
                else
                {   //  Table doesn't have any data in
                    return false;
                }
            }
        }

        private bool AreTombstoneRecords(IBlock block, TransactionContext tx)
        {
            var recordIds = block.Project(
                new object?[1],
                [block.TableSchema.RecordIdColumnIndex],
                Enumerable.Range(0, block.RecordCount),
                0)
                .Select(b => (long)b.Span[0]!);
            var tombstoneCount = TombstoneTable.Query(tx)
                .Where(pf => pf.In(t => t.DeletedRecordId, recordIds))
                .Count();
            var areTombstoneRecords = tombstoneCount != 0;

            return areTombstoneRecords;
        }

        private void TrimBlocks(BlockBuilder blockBuilder, BlockBuilder tombstoneBuilder)
        {   //  All record IDs in block
            var allRecordIds = ((IBlock)blockBuilder).Project(
                new object?[1],
                [((IBlock)blockBuilder).TableSchema.RecordIdColumnIndex],
                Enumerable.Range(0, ((IBlock)blockBuilder).RecordCount),
                0)
                .Select(b => (long)b.Span[0]!)
                .Distinct()
                .ToImmutableArray();
            //  Tombstone row index of found ids
            var matchingTombstoneRowIndexes = ((IBlock)tombstoneBuilder).Filter(
                new ConjunctionPredicate(
                    new BinaryOperatorPredicate(
                        TombstoneTable.Schema.GetColumnIndexSubset(t => t.TableName).First(),
                        ((IBlock)blockBuilder).TableSchema.TableName,
                        BinaryOperator.Equal),
                    new InPredicate(
                        TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId).First(),
                        allRecordIds.Cast<object?>())),
                false).RowIndexes
                .ToImmutableArray();
            var matchingDeletedRecordIds = ((IBlock)tombstoneBuilder).Project(
                new object?[1],
                ImmutableArray.Create(TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId).First()),
                matchingTombstoneRowIndexes,
                0)
                .Select(b => (long)b.Span[0]!);

            blockBuilder.DeleteRecordsByRecordId(matchingDeletedRecordIds);
            tombstoneBuilder.DeleteRecordsByRecordIndex(matchingTombstoneRowIndexes);
        }

        private void CommitMerge(
            IBlock block,
            IBlock? tombstoneBlock,
            TransactionContext tx)
        {
            Database.ChangeDatabaseState(state =>
            {
                var stateMap = state.InMemoryDatabase.TransactionTableLogsMap;
                var txMap = tx.TransactionState.InMemoryDatabase.TransactionTableLogsMap;
                var stateTableBlocks = stateMap[block.TableSchema.TableName].InMemoryBlocks;
                var txTableBlocks = txMap[block.TableSchema.TableName].InMemoryBlocks;
                var newPrefixBlocks = stateTableBlocks
                .Skip(txTableBlocks.Count);

                //  Record table
                if (block.RecordCount > 0)
                {
                    stateMap = stateMap.SetItem(
                        block.TableSchema.TableName,
                        new(newPrefixBlocks.Append(block).ToImmutableArray()));
                }
                else if (newPrefixBlocks.Any())
                {
                    stateMap = stateMap.SetItem(
                        block.TableSchema.TableName,
                        new(newPrefixBlocks.ToImmutableArray()));
                }
                else
                {
                    stateMap = stateMap.Remove(block.TableSchema.TableName);
                }
                //  Record tombstone
                if (tombstoneBlock != null)
                {
                    var stateTombstoneBlocks = stateMap[TombstoneTable.Schema.TableName].InMemoryBlocks;
                    var txTombstoneBlocks = txMap[TombstoneTable.Schema.TableName].InMemoryBlocks;
                    var newPrefixTombstoneBlocks = stateTombstoneBlocks
                    .Skip(txTombstoneBlocks.Count);

                    if (tombstoneBlock.RecordCount > 0)
                    {
                        stateMap = stateMap.SetItem(
                            TombstoneTable.Schema.TableName,
                            new(newPrefixTombstoneBlocks.Append(tombstoneBlock).ToImmutableArray()));
                    }
                    else if (newPrefixTombstoneBlocks.Any())
                    {
                        stateMap = stateMap.SetItem(
                            TombstoneTable.Schema.TableName,
                            new(newPrefixTombstoneBlocks.ToImmutableArray()));
                    }
                    else
                    {
                        stateMap = stateMap.Remove(TombstoneTable.Schema.TableName);
                    }
                }

                return state with
                {
                    InMemoryDatabase = state.InMemoryDatabase with
                    {
                        TransactionTableLogsMap = stateMap
                    }
                };
            });
        }
    }
}