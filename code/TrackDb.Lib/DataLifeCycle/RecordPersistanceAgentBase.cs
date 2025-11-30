using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class RecordPersistanceAgentBase : DataLifeCycleAgentBase
    {
        protected RecordPersistanceAgentBase(Database database)
            : base(database)
        {
        }

        public override void Run(
            DataManagementActivity forcedActivity,
            TransactionContext tx)
        {
            while (true)
            {
                var tableName = FindMergedCandidate(forcedActivity, tx);

                if (tableName != null)
                {
                    PersistTable(tableName, tx);
                }
                else
                {
                    return;
                }
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

                    var blockId = Database.PersistBlock(buffer.AsSpan().Slice(0, blockStats.Size), tx);

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

        protected abstract IEnumerable<Table> GetTables(
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

                foreach (var table in GetTables(forcedActivity, tx))
                {
                    var sortColumn = new SortColumn(table.Schema.CreationTimeColumnIndex, false);
                    var oldestRecord = table.Query(tx)
                        .WithInMemoryOnly()
                        .WithSortColumns([sortColumn])
                        .WithProjection([table.Schema.CreationTimeColumnIndex])
                        .Take(1)
                        .FirstOrDefault();

                    if (oldestRecord.Length == 1)   //  If no record, the default is an empty memory
                    {
                        var creationTime = (DateTime)oldestRecord.Span[0]!;

                        if (creationTime < oldestCreationTime)
                        {
                            oldestCreationTime = creationTime;
                            oldestTableName = table.Schema.TableName;
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
            var tables = GetTables(forcedActivity, tx);
            var totalRecords = tables
                .Select(t => t.Query(tx).WithInMemoryOnly().Count())
                .Sum();

            return totalRecords > MaxInMemoryDataRecords
                || (totalRecords > 0 && DoPersistAll(forcedActivity));
        }
        #endregion
    }
}