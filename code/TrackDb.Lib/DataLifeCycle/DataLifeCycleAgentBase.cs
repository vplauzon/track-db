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

                    if (map.ContainsKey(TombstoneTable.Schema.TableName))
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
        {
            var recordIds = ((IBlock)blockBuilder).Project(
                new object?[1],
                [((IBlock)blockBuilder).TableSchema.RecordIdColumnIndex],
                Enumerable.Range(0, ((IBlock)blockBuilder).RecordCount),
                0)
                .Select(b => b.Span[0])
                .ToImmutableArray();
            var tombstoneRowIndexes = ((IBlock)tombstoneBuilder).Filter(
                new ConjunctionPredicate(
                    new BinaryOperatorPredicate(
                        TombstoneTable.Schema.GetColumnIndexSubset(t => t.TableName).First(),
                        ((IBlock)blockBuilder).TableSchema.TableName,
                        BinaryOperator.Equal),
                    new InPredicate(
                        TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId).First(),
                        recordIds)),
                false).RowIndexes;
            var deletedRecordIds = ((IBlock)tombstoneBuilder).Project(
                new object?[1],
                TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId),
                tombstoneRowIndexes,
                0)
                .Select(b => (long)b.Span[0]!);

            blockBuilder.DeleteRecordsByRecordId(deletedRecordIds);
            tombstoneBuilder.DeleteRecordsByRecordIndex(tombstoneRowIndexes);
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