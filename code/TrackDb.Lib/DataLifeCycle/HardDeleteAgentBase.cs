using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

            using (var tx = Database.CreateDummyTransaction())
            {
                int blockId = GetBlockId(table, recordId, tx);
                var metadataRecord = GetMetadataRecord(tableName, blockId, tx);
                var serializedBlockMetadata = SerializedBlockMetaData.FromMetaDataRecord(
                    metadataRecord.Record,
                    out var serializedBlockId);
                var block = Database.GetOrLoadBlock(blockId, table.Schema, serializedBlockMetadata);
                (var trimmedBlock, var trimmedTombstone) = TrimRecords(block, tx);

                //  Delete meta data entry (for the deleted block)
                trimmedTombstone.AppendRecord(
                    Database.NewRecordId(),
                    TombstoneTable.Schema.FromObjectToColumns(new TombstoneRecord(
                        metadataRecord.MetadataRecordId,
                        metadataRecord.MetadataBlockId,
                        metadataRecord.MetadataTable.Schema.TableName,
                        DateTime.Now)));
                Database.ChangeDatabaseState(state =>
                {
                    var map = state.InMemoryDatabase.TableTransactionLogsMap;

                    //  Record table
                    if (((IBlock)trimmedBlock).RecordCount == 0)
                    {
                        //  Nothing
                    }
                    else if (map.ContainsKey(tableName))
                    {
                        map = map.SetItem(
                            tableName,
                            map[tableName] with
                            {
                                InMemoryBlocks = map[tableName].InMemoryBlocks.Add(trimmedBlock)
                            });
                    }
                    else // Map doesn't have the table, but we do have data
                    {
                        map = map.SetItem(tableName, new(trimmedBlock));
                    }
                    //  Tombstone table
                    map = map.SetItem(
                        TombstoneTable.Schema.TableName,
                        new(trimmedTombstone));

                    return state with
                    {
                        //  Discard the block ID
                        DiscardedBlockIds = state.DiscardedBlockIds.Add(blockId),
                        InMemoryDatabase = state.InMemoryDatabase with
                        {
                            TableTransactionLogsMap = map
                        }
                    };
                });
            }
        }

        private (BlockBuilder TrimmedBlock, BlockBuilder TrimmedTombstone) TrimRecords(
            IBlock block,
            TransactionContext tx)
        {
            var trimmedBlock = new BlockBuilder(block.TableSchema);
            var trimmedTombstone = new BlockBuilder(TombstoneTable.Schema);
            var tableDeletedRecordIds = TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, block.TableSchema.TableName))
                .Select(t => t.DeletedRecordId);

            trimmedBlock.AppendBlock(block);
            foreach (var tombstoneBlock in tx.TransactionState.InMemoryDatabase
                .TableTransactionLogsMap[TombstoneTable.Schema.TableName]
                .InMemoryBlocks)
            {
                trimmedTombstone.AppendBlock(tombstoneBlock);
            }

            var hardDeletedRecordIds = trimmedBlock.DeleteRecordsByRecordId(tableDeletedRecordIds);
            //  Let's find the tombstone record index of those record Ids
            var tombstoneIndexes = ((IBlock)trimmedTombstone).Filter(
                new ConjunctionPredicate(
                    new BinaryOperatorPredicate(
                        TombstoneTable.Schema.GetColumnIndexSubset(t => t.TableName).First(),
                        block.TableSchema.TableName,
                        BinaryOperator.Equal),
                    new InPredicate(
                        TombstoneTable.Schema.GetColumnIndexSubset(t => t.DeletedRecordId).First(),
                        hardDeletedRecordIds.Cast<object?>())),
                false).RowIndexes;

            trimmedTombstone.DeleteRecordsByRecordIndex(tombstoneIndexes);

            return (trimmedBlock, trimmedTombstone);
        }

        private static int GetBlockId(
            Table table,
            long recordId,
            TransactionContext tx)
        {
            var recordIdRecord = table.Query(tx)
                .WithIgnoreDeleted()
                //  Where recordID
                .WithPredicate(new BinaryOperatorPredicate(
                    table.Schema.Columns.Count,
                    recordId,
                    BinaryOperator.Equal))
                //  Only project the block ID
                .WithProjection([table.Schema.BlockIdColumnIndex])
                .FirstOrDefault();

            if (recordIdRecord.Length == 0)
            {
                throw new InvalidDataException(
                    $"Can't load record '{recordId}' on table '{table.Schema.TableName}'");
            }
            var blockId = (int)recordIdRecord.Span[0]!;

            return blockId;
        }

        private MetadataRecord GetMetadataRecord(
            string tableName,
            int blockId,
            TransactionContext tx)
        {
            var metaDataTable = Database.GetMetaDataTable(tableName);
            var projectionColumnIndexes = Enumerable.Range(0, metaDataTable.Schema.Columns.Count)
                .Append(metaDataTable.Schema.RecordIdColumnIndex)
                .Append(metaDataTable.Schema.BlockIdColumnIndex);
            var metaDataRecord = metaDataTable.Query(tx)
                //  We're looking for the block ID in the meta data table
                .WithPredicate(
                new BinaryOperatorPredicate(
                    metaDataTable.Schema.FindColumnIndex(MetadataColumns.BLOCK_ID),
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
        #endregion
    }
}