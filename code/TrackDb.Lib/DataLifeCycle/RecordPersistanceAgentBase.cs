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
        private record Candidate(Table Table, DateTime? OldestCreationTime);

        protected RecordPersistanceAgentBase(Database database)
            : base(database)
        {
        }

        protected void RunPersistence(DataManagementActivity forcedActivity, TransactionContext tx)
        {
            while (true)
            {
                var table = FindCandidate(forcedActivity, tx);

                if (table != null)
                {
                    PersistTable(table, tx);
                }
                else
                {
                    return;
                }
            }
        }

        private void PersistTable(Table table, TransactionContext tx)
        {   //  We persist as much blocks from the table as possible
            var tableBlockBuilder = tx.TransactionState.UncommittedTransactionLog
                .TransactionTableLogMap[table.Schema.TableName]
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
        private Table? FindCandidate(
            DataManagementActivity forcedActivity,
            TransactionContext tx)
        {
            DateTime? GetOldestRecord(Table table)
            {
                var sortColumn = new SortColumn(table.Schema.CreationTimeColumnIndex, false);
                var oldestRecord = table.Query(tx)
                    .WithInMemoryOnly()
                    .WithCommittedOnly()
                    .WithSortColumns([sortColumn])
                    .WithProjection([table.Schema.CreationTimeColumnIndex])
                    .Take(1);

                return oldestRecord.Any()
                    ? (DateTime)oldestRecord.First().Span[0]!
                    : null;
            }

            //  Should we persist any data given the total number of records in memory (across tables)?
            if (IsPersistanceRequired(forcedActivity, tx))
            {   //  Find the oldest record across tables (by creation time)
                var candidates = GetTables(forcedActivity, tx)
                    .Select(t => new Candidate(t, GetOldestRecord(t)))
                    .Where(o => o.OldestCreationTime != null)
                    .OrderBy(o => o.OldestCreationTime)
                    .ToList();

                while (candidates.Any())
                {
                    var candidate = candidates.First();

                    candidates.RemoveAt(0);
                    tx.LoadCommittedBlocksInTransaction(candidate.Table.Schema.TableName);

                    var newOldestCreationTime = GetOldestRecord(candidate.Table);

                    if (newOldestCreationTime == candidate.OldestCreationTime)
                    {
                        return candidate.Table;
                    }
                    else if (newOldestCreationTime != null)
                    {
                        candidates.Add(new Candidate(candidate.Table, newOldestCreationTime));
                        candidates.Sort();
                    }
                    else
                    {   //  newOldestCreationTime == null
                        //  Nothing, candidate is gone
                    }
                }
            }

            return null;
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