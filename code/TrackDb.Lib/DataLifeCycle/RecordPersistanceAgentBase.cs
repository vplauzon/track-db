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
        public RecordPersistanceAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedActivity)
        {
            var tableName = FindMergedCandidate(forcedActivity);

            if (tableName != null)
            {   //  We will persist blocks from the table
                using (var tx = Database.CreateDummyTransaction())
                {
                    var inMemoryDatabase = tx.TransactionState.InMemoryDatabase;
                    var tableBlock =
                        inMemoryDatabase.TransactionTableLogsMap[tableName].InMemoryBlocks.First();
                    var newTableBlock = new BlockBuilder(tableBlock.TableSchema);
                    var metadataTable =
                        Database.GetMetaDataTable(tableBlock.TableSchema.TableName);
                    var metaSchema = (MetadataTableSchema)metadataTable.Schema;
                    var metadataBlock = new BlockBuilder(metadataTable.Schema);
                    var isFirstBlockToPersist = true;
                    var buffer = new byte[StorageManager.BlockSize];
                    var skipRows = 0;

                    newTableBlock.AppendBlock(tableBlock);
                    newTableBlock.OrderByRecordId();
                    while (((IBlock)newTableBlock).RecordCount - skipRows > 0)
                    {
                        var blockStats = newTableBlock.TruncateSerialize(buffer, skipRows);

                        if (blockStats.ItemCount == 0)
                        {
                            throw new InvalidDataException(
                                $"A single record is too large to persist on table " +
                                $"'{tableBlock.TableSchema.TableName}' with " +
                                $"{tableBlock.TableSchema.Columns.Count} columns");
                        }
                        //  We stop before persisting the last (typically incomplete) block
                        if (isFirstBlockToPersist
                            || ((IBlock)newTableBlock).RecordCount - skipRows > blockStats.ItemCount)
                        {
                            var blockId = Database.GetFreeBlockId();

                            if (blockStats.Size > buffer.Length)
                            {
                                throw new InvalidOperationException(
                                    $"Block size ({blockStats.Size}) is bigger than planned" +
                                    $"maximum ({buffer.Length})");
                            }
                            StorageManager.WriteBlock(
                                blockId,
                                buffer.AsSpan().Slice(0, blockStats.Size));
                            metadataBlock.AppendRecord(
                                DateTime.Now,
                                metadataTable.NewRecordId(),
                                metaSchema.CreateMetadataRecord(
                                    blockStats.ItemCount,
                                    blockStats.Size,
                                    blockId,
                                    blockStats.Columns.Select(c => c.ColumnMinimum),
                                    blockStats.Columns.Select(c => c.ColumnMaximum)));
                            isFirstBlockToPersist = false;
                            skipRows += blockStats.ItemCount;
                        }
                        else
                        {   //  We're done
                            break;
                        }
                    }
                    newTableBlock.DeleteRecordsByRecordIndex(Enumerable.Range(0, skipRows));
                    CommitPersistance(newTableBlock, metadataBlock, tx);
                }
            }

            return tableName == null;
        }

        protected abstract int MaxInMemoryDataRecords { get; }

        protected abstract IEnumerable<KeyValuePair<string, ImmutableTableTransactionLogs>> GetTableLogs(
            DataManagementActivity forcedActivity,
            TransactionContext tx);

        protected abstract bool DoPersistAll(DataManagementActivity forcedActivity);

        #region Candidates
        private string? FindMergedCandidate(DataManagementActivity forcedActivity)
        {
            string? tableName = FindUnmergedCandidate(forcedActivity);

            while (tableName != null)
            {
                if (MergeTableTransactionLogs(tableName))
                {
                    var newTableName = FindUnmergedCandidate(forcedActivity);

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

        private string? FindUnmergedCandidate(DataManagementActivity forcedActivity)
        {
            using (var tx = Database.CreateTransaction())
            {
                var inMemoryDb = tx.TransactionState.InMemoryDatabase;

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
                        {   //  Fetch the record ID
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

        private void CommitPersistance(
            IBlock tableBlock,
            IBlock metadataBlock,
            TransactionContext tx)
        {
            Database.ChangeDatabaseState(state =>
            {
                var inMemoryDatabase = state.InMemoryDatabase;
                var tableTransactionLogsMap = inMemoryDatabase.TransactionTableLogsMap;
                var tableLogs = tableTransactionLogsMap[tableBlock.TableSchema.TableName];
                var metadataTableLogs = tableTransactionLogsMap.ContainsKey(metadataBlock.TableSchema.TableName)
                ? tableTransactionLogsMap[metadataBlock.TableSchema.TableName]
                : new ImmutableTableTransactionLogs();

                //  Adjust table
                var inMemoryBlocks = tableLogs.InMemoryBlocks.Skip(1);

                inMemoryBlocks = tableBlock.RecordCount > 0
                ? inMemoryBlocks.Prepend(tableBlock)
                : inMemoryBlocks;

                if (inMemoryBlocks.Any())
                {
                    tableTransactionLogsMap = tableTransactionLogsMap.SetItem(
                        tableBlock.TableSchema.TableName,
                        tableLogs with
                        {
                            InMemoryBlocks = inMemoryBlocks.ToImmutableArray()
                        });
                }
                else
                {
                    tableTransactionLogsMap = tableTransactionLogsMap.Remove(
                        tableBlock.TableSchema.TableName);
                }
                //  Adjust metadata table
                tableTransactionLogsMap = tableTransactionLogsMap.SetItem(
                    metadataBlock.TableSchema.TableName,
                    metadataTableLogs with
                    {
                        InMemoryBlocks = metadataTableLogs.InMemoryBlocks
                        .Append(metadataBlock)
                        .ToImmutableArray()
                    });
                inMemoryDatabase = inMemoryDatabase with
                {
                    TransactionTableLogsMap = tableTransactionLogsMap
                };
                state = state with { InMemoryDatabase = inMemoryDatabase };

                return state;
            });
        }
    }
}