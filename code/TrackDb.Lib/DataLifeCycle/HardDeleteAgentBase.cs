using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class HardDeleteAgentBase : DataLifeCycleAgentBase
    {
        #region Inner types
        protected record TableRecord(string TableName, long RecordId);

        private record MetadataRecord(
            Table MetadataTable,
            long MetadataRecordId,
            int? MetadataBlockId,
            ReadOnlyMemory<object?> Record);
        #endregion

        public HardDeleteAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        public override bool Run(DataManagementActivity forcedDataManagementActivity)
        {
            var doHardDeleteAll =
                (forcedDataManagementActivity & DataManagementActivity.HardDeleteAll) != 0;
            var candidate = FindMergedRecordCandidate(doHardDeleteAll);

            if (candidate != null)
            {
                HardDelete(candidate.TableName, candidate.RecordId);

                return false;
            }
            else
            {
                return true;
            }
        }

        protected abstract TableRecord? FindUnmergedRecordCandidate(bool doHardDeleteAll);

        private TableRecord? FindMergedRecordCandidate(bool doHardDeleteAll)
        {
            TableRecord? tableRecord = FindUnmergedRecordCandidate(doHardDeleteAll);

            while (tableRecord != null)
            {
                if (MergeTableTransactionLogs(tableRecord.TableName))
                {
                    var newTableRecord = FindUnmergedRecordCandidate(doHardDeleteAll);

                    if (newTableRecord == tableRecord)
                    {
                        return tableRecord;
                    }
                    else
                    {
                        tableRecord = newTableRecord;
                        //  Re-loop if null, otherwise will return null
                    }
                }
                else
                {
                    return tableRecord;
                }
            }

            return null;
        }

        #region Hard Delete
        private void HardDelete(string tableName, long recordId)
        {
            var table = Database.GetAnyTable(tableName);
            var blockId = GetBlockId(table, recordId);

            if (blockId != null)
            {
                using (var tx = Database.CreateTransaction())
                {
                    var metadataRecord = GetMetadataRecord(tableName, blockId.Value, tx);
                    var serializedBlockMetadata = SerializedBlockMetaData.FromMetaDataRecord(
                        metadataRecord.Record);
                    var block = Database.GetOrLoadBlock(blockId.Value, table.Schema);

                    tx.TransactionState.UncommittedTransactionLog.AppendBlock(block);
                    DeleteMetadataRecord(metadataRecord, tx);
                    DiscardBlock(blockId.Value);

                    tx.Complete();
                }
                MergeTableTransactionLogs(tableName);
            }
            else
            {   //  Missing record:  likely 2 transactions (snapshot) in parallel
                DeleteTombstoneRecord(tableName, recordId);
            }
        }

        private void DeleteMetadataRecord(MetadataRecord metadataRecord, TransactionContext tx)
        {
            var deletedCount = metadataRecord.MetadataTable.Query(tx)
                .WithPredicate(new BinaryOperatorPredicate(
                    metadataRecord.MetadataTable.Schema.RecordIdColumnIndex,
                    metadataRecord.MetadataRecordId,
                    BinaryOperator.Equal))
                .Delete();

            if (deletedCount != 1)
            {
                throw new InvalidDataException(
                    $"Couldn't delete metadata record {metadataRecord.MetadataRecordId} in" +
                    $"table {metadataRecord.MetadataTable.Schema.TableName}");
            }
        }

        private void DiscardBlock(int blockId)
        {
            Database.ChangeDatabaseState(state =>
            {
                return state with
                {
                    //  Discard the block ID
                    DiscardedBlockIds = state.DiscardedBlockIds.Add(blockId)
                };
            });
        }

        private int? GetBlockId(Table table, long recordId)
        {
            using (var tx = Database.CreateDummyTransaction())
            {
                var recordIdRecord = table.Query(tx)
                    .WithIgnoreDeleted()
                    //  Where recordID
                    .WithPredicate(new BinaryOperatorPredicate(
                        table.Schema.Columns.Count,
                        recordId,
                        BinaryOperator.Equal))
                    //  Only project the block ID
                    .WithProjection([table.Schema.ParentBlockIdColumnIndex])
                    .FirstOrDefault();

                if (recordIdRecord.Length == 0)
                {   //  This is the case where two transactions deleted the same record
                    return null;
                }
                else
                {
                    var blockId = (int)recordIdRecord.Span[0]!;

                    return blockId;
                }
            }
        }

        private MetadataRecord GetMetadataRecord(
            string tableName,
            int blockId,
            TransactionContext tx)
        {
            var metaDataTable = Database.GetMetaDataTable(tableName);
            var metadataTableSchema = (MetadataTableSchema)metaDataTable.Schema;
            var projectionColumnIndexes = Enumerable.Range(0, metaDataTable.Schema.Columns.Count)
                .Append(metaDataTable.Schema.RecordIdColumnIndex)
                .Append(metaDataTable.Schema.ParentBlockIdColumnIndex);
            var metaDataRecord = metaDataTable.Query(tx)
                //  We're looking for the block ID in the meta data table
                .WithPredicate(
                new BinaryOperatorPredicate(
                    metadataTableSchema.BlockIdColumnIndex,
                    blockId,
                    BinaryOperator.Equal))
                //  Return record-ID & block-ID as well
                .WithProjection(projectionColumnIndexes)
                .WithTake(1)
                .FirstOrDefault();

            if (metaDataRecord.Length == 0)
            {
                throw new InvalidDataException(
                    $"Can't load block '{blockId}' on table '{tableName}'");
            }

            var metadataRecordId = (long)metaDataRecord.Span[metaDataRecord.Length - 2]!;
            var metadataBlockId = (int?)metaDataRecord.Span[metaDataRecord.Length - 1];

            return new(
                metaDataTable,
                metadataRecordId,
                metadataBlockId <= 0 ? null : metadataBlockId,
                metaDataRecord.Slice(0, metaDataRecord.Length - 2));
        }

        private void DeleteTombstoneRecord(string tableName, long deletedRecordId)
        {
            var inMemoryDatabase = Database.GetDatabaseStateSnapshot().InMemoryDatabase;
            //  We merged the table previously so we know the record is in the first log
            var block = inMemoryDatabase
                .TableTransactionLogsMap[TombstoneTable.Schema.TableName]
                .InMemoryBlocks
                .First();
            var builder = new BlockBuilder(block.TableSchema);

            builder.AppendBlock(block);

            var rowIndexes = ((IBlock)builder).Filter(
                new ConjunctionPredicate(
                    new BinaryOperatorPredicate(
                        TombstoneTable.Schema.GetColumnIndexSubset(t => t.TableName).First(),
                        tableName,
                        BinaryOperator.Equal),
                    new BinaryOperatorPredicate(
                        TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId).First(),
                        deletedRecordId,
                        BinaryOperator.Equal)
                    ),
                false)
                .RowIndexes;

            builder.DeleteRecordsByRecordIndex(rowIndexes);
            Database.ChangeDatabaseState(state =>
            {
                var map = state.InMemoryDatabase.TableTransactionLogsMap;
                var logs = map[TombstoneTable.Schema.TableName];
                var newLogs = logs.InMemoryBlocks
                    .Skip(1)
                    .Append(builder)
                    .ToImmutableArray();

                map = map.SetItem(
                    TombstoneTable.Schema.TableName,
                    new(newLogs));

                return state with
                {
                    InMemoryDatabase = state.InMemoryDatabase with
                    {
                        TableTransactionLogsMap = map
                    }
                };
            });
        }
        #endregion
    }
}