using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class RecordPersistanceAgentBase : DataLifeCycleAgentBase
    {
        protected RecordPersistanceAgentBase(Database database)
            : base(database)
        {
        }

        public override bool Run(DataManagementActivity forcedActivity)
        {
            using (var tx = Database.CreateTransaction())
            {
                var tableName = FindMergedCandidate(forcedActivity, tx);

                if (tableName != null)
                {
                    PersistTable(tableName, tx);
                }

                tx.Complete();

                return tableName == null;
            }
        }

        private void PersistTable(string tableName, TransactionContext tx)
        {   //  We persist as much blocks from the table as possible
            var tableBlockBuilder = tx.TransactionState.UncommittedTransactionLog
                .TransactionTableLogMap[tableName]
                .CommittedDataBlock;

            if (tableBlockBuilder == null)
            {
                throw new InvalidOperationException("CommittedDataBlock shouldn't be null");
            }

            IBlock tableBlock = tableBlockBuilder;
            var metadataTable = Database.GetMetaDataTable(tableBlock.TableSchema.TableName);
            var metaSchema = (MetadataTableSchema)metadataTable.Schema;
            var isFirstBlockToPersist = true;
            var buffer = new byte[Database.DatabasePolicy.StoragePolicy.BlockSize];
            var skipRows = 0;

            tableBlockBuilder.OrderByRecordId();
            while (tableBlock.RecordCount - skipRows > 0)
            {
                var blockStats = tableBlockBuilder.TruncateSerialize(buffer, skipRows);

                if (blockStats.ItemCount == 0)
                {
                    throw new InvalidDataException(
                        $"A single record is too large to persist on table " +
                        $"'{tableBlock.TableSchema.TableName}' with " +
                        $"{tableBlock.TableSchema.Columns.Count} columns");
                }
                if (blockStats.Size > buffer.Length)
                {
                    throw new IndexOutOfRangeException(
                        $"Buffer overrun:  {blockStats.Size}>{buffer.Length}");
                }

                //  We stop before persisting the last (typically incomplete) block
                if (isFirstBlockToPersist
                    || tableBlock.RecordCount - skipRows > blockStats.ItemCount)
                {
                    if (blockStats.Size > buffer.Length)
                    {
                        throw new InvalidOperationException(
                            $"Block size ({blockStats.Size}) is bigger than planned" +
                            $"maximum ({buffer.Length})");
                    }

                    var blockId = Database.PersistBlock(buffer.AsSpan().Slice(0, blockStats.Size));

                    metadataTable.AppendRecord(
                        metaSchema.CreateMetadataRecord(blockId, blockStats).Span,
                        tx);
                    isFirstBlockToPersist = false;
                    skipRows += blockStats.ItemCount;
                }
                else
                {   //  We're done
                    break;
                }
            }
            tableBlockBuilder.DeleteRecordsByRecordIndex(Enumerable.Range(0, skipRows));
        }

        protected abstract int MaxInMemoryDataRecords { get; }

        protected abstract IEnumerable<KeyValuePair<string, ImmutableTableTransactionLogs>> GetTableLogs(
            DataManagementActivity forcedActivity,
            TransactionContext tx);

        protected abstract bool DoPersistAll(DataManagementActivity forcedActivity);

        #region Candidates
        private string? FindMergedCandidate(
            DataManagementActivity forcedActivity,
            TransactionContext tx)
        {
            string? tableName = FindUnmergedCandidate(forcedActivity, tx);

            while (tableName != null)
            {
                if (tx.LoadCommittedBlocksInTransaction(tableName))
                {
                    var newTableName = FindUnmergedCandidate(forcedActivity, tx);

                    if (newTableName == tableName)
                    {
                        return tableName;
                    }
                    else
                    {
                        tableName = newTableName;
                        //  Re-loop if null, otherwise will return null
                    }
                }
                else
                {
                    return tableName;
                }
            }

            return null;
        }

        private string? FindUnmergedCandidate(
            DataManagementActivity forcedActivity,
            TransactionContext tx)
        {
            //  Should we persist any data given the total number of records in memory (across tables)?
            if (IsPersistanceRequired(forcedActivity, tx))
            {   //  Find the oldest record across tables (by creation time)
                var oldestCreationTime = DateTime.MaxValue;
                var oldestTableName = (string?)null;
                var buffer = new object?[1];
                var rowIndexes = new[] { 0 };
                var tableMap = Database.GetDatabaseStateSnapshot().TableMap;

                foreach (var pair in GetTableLogs(forcedActivity, tx))
                {
                    var tableName = pair.Key;
                    var logs = pair.Value;
                    var table = Database.GetAnyTable(tableName);
                    var blocks = logs.InMemoryBlocks
                        .Where(b => b.RecordCount > 0);

                    foreach (var block in blocks)
                    {   //  Fetch the creation time
                        var projectedColumns =
                            ImmutableArray.Create(block.TableSchema.CreationTimeColumnIndex);

                        var blockOldestCreationTime = block.Project(buffer, projectedColumns, rowIndexes, 0)
                            .Select(r => (DateTime)r.Span[0]!)
                            .Min();

                        if (blockOldestCreationTime < oldestCreationTime)
                        {
                            oldestCreationTime = blockOldestCreationTime;
                            oldestTableName = tableName;
                        }
                    }
                }

                return oldestTableName;
            }
            else
            {
                return null;
            }
        }

        private bool IsPersistanceRequired(
            DataManagementActivity forcedActivity,
            TransactionContext tx)
        {
            var tableLogs = GetTableLogs(forcedActivity, tx);
            var totalRecords = tableLogs
                .Select(p => p.Value)
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            return totalRecords > MaxInMemoryDataRecords
                || (totalRecords > 0 && DoPersistAll(forcedActivity));
        }
        #endregion
    }
}