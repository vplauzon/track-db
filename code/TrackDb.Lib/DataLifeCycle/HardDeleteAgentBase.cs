using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
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
    internal abstract class HardDeleteAgentBase : DataLifeCycleAgentBase
    {
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
            var candidate = FindRecordCandidate(doHardDeleteAll);

            if (candidate != null)
            {
                HardDelete(candidate.Value.TableName, candidate.Value.RecordId);

                return false;
            }
            else
            {
                return true;
            }
        }

        protected abstract (string TableName, long RecordId)? FindRecordCandidate(
            bool doHardDeleteAll);

        #region Hard Delete
        private void HardDelete(string tableName, long recordId)
        {
            var table = Database.GetAnyTable(tableName);

            using (var tx = Database.CreateDummyTransaction())
            {
                int blockId = GetBlockId(table, recordId, tx);
                var metadataRecord = GetMetadataRecord(tableName, blockId, tx);
                var serializedBlockMetadata = SerializedBlockMetaData.FromMetaDataRecord(
                    metadataRecord,
                    out var serializedBlockId);
                var block = Database.GetOrLoadBlock(blockId, table.Schema, serializedBlockMetadata);
                (var trimmedBlock, var trimmedTombstone) = TrimRecords(block, tx);

                //  Delete meta data entry (for the deleted block)
                trimmedTombstone.AppendRecord(
                    Database.NewRecordId(),
                    TombstoneTable.Schema.FromObjectToColumns(new TombstoneRecord(
                        (long)metadataRecord.Span[metadataRecord.Length - 1]!,
                        blockId,
                        tableName,
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
                //  Where recordID
                .WithPredicate(new BinaryOperatorPredicate(
                    table.Schema.Columns.Count,
                    recordId,
                    BinaryOperator.Equal))
                //  Only project the block ID
                .WithProjection([table.Schema.Columns.Count + 1])
                .FirstOrDefault();

            if (recordIdRecord.Length == 0)
            {
                throw new InvalidDataException(
                    $"Can't load record '{recordId}' on table '{table.Schema.TableName}'");
            }
            var blockId = (int)recordIdRecord.Span[0]!;

            return blockId;
        }

        private ReadOnlyMemory<object?> GetMetadataRecord(
            string tableName,
            int blockId,
            TransactionContext tx)
        {
            var metaDataTable = Database.GetMetaDataTable(tableName);
            var metaDataRecord = metaDataTable.Query(tx)
                //  We're looking for the block ID in the meta data table
                .WithPredicate(
                new BinaryOperatorPredicate(
                    metaDataTable.Schema.FindColumnIndex(MetadataColumns.BLOCK_ID),
                    blockId,
                    BinaryOperator.Equal))
                //  Return record-ID as well
                .WithProjection(Enumerable.Range(0, metaDataTable.Schema.Columns.Count + 1))
                .WithTake(1)
                .FirstOrDefault();

            if (metaDataRecord.Length == 0)
            {
                throw new InvalidDataException(
                    $"Can't load block '{blockId}' on table '{tableName}'");
            }

            return metaDataRecord;
        }
        #endregion
    }
}