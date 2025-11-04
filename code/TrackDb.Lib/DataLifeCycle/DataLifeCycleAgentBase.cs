using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
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
                var map = tx.TransactionState.InMemoryDatabase.TableTransactionLogsMap;

                if (map.ContainsKey(tableName))
                {
                    var logs = map[tableName];

                    if (logs.InMemoryBlocks.Count == 1
                        && !AreTombstoneRecords(logs.InMemoryBlocks.First(), tx))
                    {
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
                {
                    return false;
                }
            }
        }

        private bool AreTombstoneRecords(IBlock block, TransactionContext tx)
        {
            var recordIds = block.Project(
                new object?[1],
                [block.TableSchema.Columns.Count],
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
        {   //  Fetch all tombstoned record ID for the table
            var allTombstoneRowIndexes = ((IBlock)tombstoneBuilder).Filter(
                new BinaryOperatorPredicate(
                    TombstoneTable.Schema.GetColumnIndexSubset(t => t.TableName).First(),
                    ((IBlock)blockBuilder).TableSchema.TableName,
                    BinaryOperator.Equal),
                false).RowIndexes;
            var allTombstonedRecordIdMap = ((IBlock)tombstoneBuilder).Project(
                new object?[2],
                [
                    TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId).First(),
                    TombstoneTable.Schema.RecordIdColumnIndex
                ],
                allTombstoneRowIndexes,
                0)
                .Select(b => new
                {
                    DeletedRecordId = (long)b.Span[0]!,
                    TombstoneRecordId = (long)b.Span[1]!
                })
                .ToImmutableDictionary(o => o.DeletedRecordId, o => o.TombstoneRecordId);
            //  Delete those found in the block
            var hardDeletedRecordIds = blockBuilder.DeleteRecordsByRecordId(
                allTombstonedRecordIdMap.Keys);
            //  Delete those found from the tombstone table
            var hardDeletedTombstoneRecordIds = tombstoneBuilder.DeleteRecordsByRecordId(
                hardDeletedRecordIds
                .Select(id => allTombstonedRecordIdMap[id]));

            if (hardDeletedTombstoneRecordIds.Count() != hardDeletedRecordIds.Count())
            {
                throw new InvalidOperationException("Merge logic flawed");
            }
        }

        private void CommitMerge(
            IBlock block,
            IBlock? tombstoneBlock,
            TransactionContext tx)
        {
            Database.ChangeDatabaseState(state =>
            {
                var map = state.InMemoryDatabase.TableTransactionLogsMap;

                //  Record table
                if (block.RecordCount > 0)
                {
                    map = map.SetItem(block.TableSchema.TableName, new(block));
                }
                else
                {
                    map = map.Remove(block.TableSchema.TableName);
                }
                //  Record tombstone
                if (tombstoneBlock != null)
                {
                    if (tombstoneBlock.RecordCount > 0)
                    {
                        map = map.SetItem(
                            tombstoneBlock.TableSchema.TableName,
                            new(tombstoneBlock));
                    }
                    else
                    {
                        map = map.Remove(tombstoneBlock.TableSchema.TableName);
                    }
                }

                return state with
                {
                    InMemoryDatabase = state.InMemoryDatabase with
                    {
                        TableTransactionLogsMap = map
                    }
                };
            });
        }
    }
}