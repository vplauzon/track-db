using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Cache;
using TrackDb.Lib.Cache.CachedBlock;
using TrackDb.Lib.DbStorage;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class DataLifeCycleAgentBase
    {
        private readonly Lazy<StorageManager> _storageManager;

        protected DataLifeCycleAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<StorageManager> storageManager)
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

        protected StorageManager StorageManager => _storageManager.Value;

        protected (BlockBuilder tableBlock, BlockBuilder? tombstoneBlock) MergeTableTransactionLogs(
            string tableName,
            TransactionContext tc)
        {
            var dbCache = tc.TransactionState.DatabaseCache;
            var logs = dbCache.TableTransactionLogsMap[tableName];
            var blockBuilder = logs.MergeLogs();
            var deletedRecordsIds = Database.GetDeletedRecordIds(tableName, tc);
            var actuallyDeletedRecordIds = blockBuilder.DeleteRecordsByRecordId(deletedRecordsIds)
                .ToImmutableArray();

            if (actuallyDeletedRecordIds.Any())
            {   //  We need to erase the tombstones record that were actually deleted
                var tombstoneBlockBuilder = new BlockBuilder(TombstoneTable.Schema);
                var tombstoneQueryFactory = new QueryPredicateFactory<TombstoneRecord>(TombstoneTable.Schema);

                foreach (var block in
                    tc.TransactionState.ListTransactionLogBlocks(TombstoneTable.Schema.TableName))
                {
                    tombstoneBlockBuilder.AppendBlock(block);
                }

                var tombstoneRowIndexesToRemove = ((IBlock)tombstoneBlockBuilder).Filter(
                    tombstoneQueryFactory.Equal(t => t.TableName, tableName)
                    .And(tombstoneQueryFactory.In(t => t.RecordId, actuallyDeletedRecordIds)));

                tombstoneBlockBuilder.DeleteRecordsByRecordIndex(tombstoneRowIndexesToRemove);

                return (blockBuilder, tombstoneBlockBuilder);
            }
            else
            {
                return (blockBuilder, null);
            }
        }

        protected void CommitAlteredLogs(
            IImmutableDictionary<string, BlockBuilder> tableToNewLogsMap,
            TransactionContext tc)
        {
            ImmutableTableTransactionLogs UpdateLogs(
                ImmutableTableTransactionLogs oldLogs,
                ImmutableTableTransactionLogs currentStateLogs,
                BlockBuilder newBlock)
            {
                var resultingBlocks = currentStateLogs.InMemoryBlocks
                    .Skip(oldLogs.InMemoryBlocks.Count);

                if (((IBlock)newBlock).RecordCount > 0)
                {
                    resultingBlocks = resultingBlocks.Prepend(newBlock);
                }

                return new ImmutableTableTransactionLogs(resultingBlocks.ToImmutableArray());
            }
            Database.ChangeDatabaseState(state =>
            {
                var stateCurrentMap = state.DatabaseCache.TableTransactionLogsMap;

                foreach (var pair in tableToNewLogsMap)
                {
                    var tableName = pair.Key;
                    var newBlock = pair.Value;

                    stateCurrentMap = stateCurrentMap.SetItem(
                        tableName,
                        UpdateLogs(
                            tc.TransactionState.DatabaseCache.TableTransactionLogsMap[tableName],
                            stateCurrentMap[tableName],
                            newBlock));
                }

                return state with { DatabaseCache = new DatabaseCache(stateCurrentMap) };
            });
        }
    }
}