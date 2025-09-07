using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
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

        protected void MergeTableTransactionLogs(string tableName)
        {
            using (var tc = Database.CreateDummyTransaction())
            {
                (var tableBlock, var tombstoneBlock) =
                    MergeTableTransactionLogs(tableName, tc);
                var mapBuilder = ImmutableDictionary<string, BlockBuilder>.Empty.ToBuilder();

                mapBuilder.Add(tableName, tableBlock);
                if (tombstoneBlock != null)
                {
                    mapBuilder.Add(TombstoneTable.Schema.TableName, tombstoneBlock);
                }
                CommitAlteredLogs(
                    mapBuilder.ToImmutable(),
                    ImmutableDictionary<string, BlockBuilder>.Empty,
                    tc);
            }
        }

        protected (BlockBuilder tableBlock, BlockBuilder? tombstoneBlock) MergeTableTransactionLogs(
            string tableName,
            TransactionContext tc)
        {
            var inMemoryDb = tc.TransactionState.InMemoryDatabase;
            var logs = inMemoryDb.TableTransactionLogsMap[tableName];
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
            IImmutableDictionary<string, BlockBuilder> tableToReplacedLogsMap,
            IImmutableDictionary<string, BlockBuilder> tableToAddedLogsMap,
            TransactionContext tc)
        {
            IImmutableDictionary<string, ImmutableTableTransactionLogs> UpdateLogs(
                IImmutableDictionary<string, ImmutableTableTransactionLogs> oldMap,
                IImmutableDictionary<string, ImmutableTableTransactionLogs> newMap,
                string tableName,
                BlockBuilder newBlock)
            {
                oldMap.TryGetValue(tableName, out var oldLogs);
                newMap.TryGetValue(tableName, out var newLogs);

                var resultingBlocks =
                    (newLogs?.InMemoryBlocks ?? (IEnumerable<IBlock>)Array.Empty<IBlock>())
                    .Skip(oldLogs?.InMemoryBlocks.Count ?? 0);

                if (((IBlock)newBlock).RecordCount > 0)
                {
                    resultingBlocks = resultingBlocks
                        .Prepend(newBlock);
                }
                if (resultingBlocks.Any())
                {
                    return newMap.SetItem(
                        tableName,
                        new ImmutableTableTransactionLogs(resultingBlocks.ToImmutableArray()));
                }
                else
                {
                    return newLogs == null
                        ? newMap
                        : newMap.Remove(tableName);
                }
            }
            IImmutableDictionary<string, ImmutableTableTransactionLogs> AddLogs(
                IImmutableDictionary<string, ImmutableTableTransactionLogs> oldMap,
                IImmutableDictionary<string, ImmutableTableTransactionLogs> newMap,
                string tableName,
                BlockBuilder newBlock)
            {
                if (((IBlock)newBlock).RecordCount > 0)
                {
                    oldMap.TryGetValue(tableName, out var oldLogs);
                    newMap.TryGetValue(tableName, out var newLogs);

                    var resultingBlocks =
                        (newLogs?.InMemoryBlocks ?? (IEnumerable<IBlock>)Array.Empty<IBlock>())
                        .Append(newBlock);

                    if (resultingBlocks.Any())
                    {
                        return newMap.SetItem(
                            tableName,
                            new ImmutableTableTransactionLogs(resultingBlocks.ToImmutableArray()));
                    }
                    else
                    {
                        return newLogs == null
                            ? newMap
                            : newMap.Remove(tableName);
                    }
                }
                else
                {
                    return newMap;
                }
            }

            Database.ChangeDatabaseState(state =>
            {
                var stateCurrentMap = state.InMemoryDatabase.TableTransactionLogsMap;

                foreach (var pair in tableToReplacedLogsMap)
                {
                    var tableName = pair.Key;
                    var newBlock = pair.Value;

                    stateCurrentMap = UpdateLogs(
                        tc.TransactionState.InMemoryDatabase.TableTransactionLogsMap,
                        stateCurrentMap,
                        tableName,
                        newBlock);
                }
                foreach (var pair in tableToAddedLogsMap)
                {
                    var tableName = pair.Key;
                    var newBlock = pair.Value;

                    stateCurrentMap = AddLogs(
                        tc.TransactionState.InMemoryDatabase.TableTransactionLogsMap,
                        stateCurrentMap,
                        tableName,
                        newBlock);
                }

                return state with { InMemoryDatabase = new InMemoryDatabase(stateCurrentMap) };
            });
        }
    }
}