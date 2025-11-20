using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib.DataLifeCycle
{
    internal abstract class BlockMergingAgentBase : DataLifeCycleAgentBase
    {
        #region Inner types
        protected record BlockInfo(
            MetaDataBlock? MetaDataBlock,
            bool isNewBlock,
            IBlock? ReadOnlyBlock,
            BlockBuilder? DataBlockBuilder,
            IImmutableList<int> DeletedBlockIds,
            IImmutableList<long> HardDeletedRecordIds)
        {
            public static BlockInfo FromMetadataBlock(MetaDataBlock metaDataBlock)
            {
                return new BlockInfo(
                    metaDataBlock,
                    false,
                    null,
                    null,
                    ImmutableArray<int>.Empty,
                    ImmutableArray<long>.Empty);
            }

            public BlockInfo HardDeleteRecords(
                IEnumerable<long> deletedRecordIds,
                Database database,
                TableSchema schema)
            {
                if (MetaDataBlock != null
                    && ReadOnlyBlock == null
                    && DataBlockBuilder == null)
                {
                    var canHaveTombstones = DataBlockBuilder == null
                        && deletedRecordIds
                        .Any(id => id >= MetaDataBlock.RecordIdMin
                        && id <= MetaDataBlock.RecordIdMax);

                    if (canHaveTombstones)
                    {
                        var newBlockInfo = LoadReadonlyBlock(database, schema);

                        return newBlockInfo.HardDeleteRecords(deletedRecordIds, database, schema);
                    }
                    else
                    {
                        return this;
                    }
                }
                else if (MetaDataBlock != null
                    && ReadOnlyBlock != null
                    && DataBlockBuilder == null)
                {
                    var predicate = new InPredicate(
                        schema.RecordIdColumnIndex,
                        deletedRecordIds.Cast<object?>());

                    if (ReadOnlyBlock.Filter(predicate, false).RowIndexes.Any())
                    {
                        var newBlockInfo = LoadBlockBuilder(schema);

                        return newBlockInfo.HardDeleteRecords(deletedRecordIds, database, schema);
                    }
                    else
                    {
                        return this;
                    }
                }
                else if (DataBlockBuilder != null)
                {
                    var hardDeletedRecordIds = DataBlockBuilder.DeleteRecordsByRecordId(deletedRecordIds);

                    if (hardDeletedRecordIds.Any())
                    {
                        return this with
                        {
                            HardDeletedRecordIds = HardDeletedRecordIds.AddRange(hardDeletedRecordIds)
                        };
                    }
                    else
                    {
                        return this;
                    }
                }
                else
                {
                    return this;
                }
            }

            public BlockInfo EnsurePersisted()
            {
                if (DataBlockBuilder != null)
                {
                    //var buffer = new byte[];
                    //DataBlockBuilder.Serialize();
                    throw new NotImplementedException();
                }
                else
                {
                    return this;
                }
            }

            #region Load methods
            private BlockInfo LoadReadonlyBlock(Database database, TableSchema schema)
            {
                if (MetaDataBlock == null || ReadOnlyBlock != null)
                {
                    throw new InvalidOperationException();
                }

                var block = database.GetOrLoadBlock(MetaDataBlock.BlockId, schema);

                return this with
                {
                    ReadOnlyBlock = block
                };
            }

            private BlockInfo LoadBlockBuilder(TableSchema schema)
            {
                if (MetaDataBlock == null || ReadOnlyBlock == null)
                {
                    throw new InvalidOperationException();
                }

                var blockBuilder = new BlockBuilder(schema);

                blockBuilder.AppendBlock(ReadOnlyBlock);

                return this with
                {
                    MetaDataBlock = null,
                    ReadOnlyBlock = null,
                    DataBlockBuilder = blockBuilder,
                    DeletedBlockIds = DeletedBlockIds.Add(MetaDataBlock.BlockId)
                };
            }
            #endregion
        }
        #endregion

        public BlockMergingAgentBase(
            Database database,
            TypedTable<TombstoneRecord> tombstoneTable,
            Lazy<DatabaseFileManager> storageManager)
            : base(database, tombstoneTable, storageManager)
        {
        }

        /// <summary>
        /// Compact (remove deleted records) blocks from a data table (generation 1) and merge them
        /// together, updating the meta blocks "in place".
        /// </summary>
        /// <param name="dataTableName"></param>
        /// <param name="deletedRecordId">Starting record ID in the table.</param>
        /// <param name="tx"></param>
        /// <returns></returns>
        protected bool MergeDataBlocks(string dataTableName, long deletedRecordId, TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var metadataTableName = tableMap[dataTableName].MetaDataTableName;

            if (tableMap[dataTableName].IsMetaDataTable)
            {
                throw new ArgumentException($"Table is metadata", nameof(dataTableName));
            }
            if (metadataTableName == null)
            {
                throw new ArgumentException($"Table has no corresponding metadata table", nameof(dataTableName));
            }
            var deletedRecordBlockId = FindDataBlock(dataTableName, deletedRecordId, tx);

            if (deletedRecordBlockId == null)
            {   //  Record doesn't exist anymore:  likely a racing condition between 2 transactions
                //  (should be rare)
                Database.TombstoneTable.Query(tx)
                    .Where(pf => pf.Equal(t => t.TableName, dataTableName))
                    .Where(pf => pf.Equal(t => t.DeletedRecordId, deletedRecordId))
                    .Delete();

                return true;
            }
            else
            {
                var parentBlockId = FindParentBlock(metadataTableName, deletedRecordBlockId.Value, tx);

                return MergeSubBlocks(metadataTableName, parentBlockId, tx);
            }
        }

        /// <summary>
        /// Merges all children blocks of <paramref name="metaMetaBlockId"/>.
        /// </summary>
        /// <param name="metadataTableName">
        /// Metadata table where the blocks (not <paramref name="metaMetaBlockId"/>) live
        /// </param>
        /// <param name="metaMetaBlockId"></param>
        /// <param name="tx"></param>
        /// <returns></returns>
        protected bool MergeSubBlocks(
            string metadataTableName,
            int? metaMetaBlockId,
            TransactionContext tx)
        {
            return MergeSubBlocksWithReplacements(
                metadataTableName,
                metaMetaBlockId,
                Array.Empty<int>(),
                tx);
        }

        private bool MergeSubBlocksWithReplacements(
            string metadataTableName,
            int? metaMetaBlockId,
            int[] replacements,
            TransactionContext tx)
        {
            var tableMap = Database.GetDatabaseStateSnapshot().TableMap;
            var blockInfoStack = new Stack<BlockInfo>(
                LoadBlockInfos(metadataTableName, metaMetaBlockId, tx)
                .OrderBy(b => b.MetaDataBlock!.RecordIdMax));
            var processedBlockInfos = new List<BlockInfo>(blockInfoStack.Count);
            var metaSchema = (MetadataTableSchema)tableMap[metadataTableName].Table.Schema;
            var schema = metaSchema.ParentSchema;
            var deletedRecordIds = GetTombstonedRecords(
                schema.TableName,
                blockInfoStack.Min(b => b.MetaDataBlock!.RecordIdMin),
                blockInfoStack.Max(b => b.MetaDataBlock!.RecordIdMax),
                tx);
            var hardDeletedRecordIds = new List<long>(deletedRecordIds.Count);

            while (blockInfoStack.Any())
            {
                var leftBlock = blockInfoStack.Pop().HardDeleteRecords(deletedRecordIds, Database, schema);

                if (blockInfoStack.Any())
                {
                    var rightBlock = blockInfoStack.Pop();

                    throw new NotImplementedException();
                }
                else
                {   //  We're done
                    leftBlock = leftBlock.EnsurePersisted();
                    processedBlockInfos.Add(leftBlock);
                }
            }

            HardDeleteRecords(
                schema.TableName,
                processedBlockInfos.SelectMany(b => b.HardDeletedRecordIds),
                tx);
            DeleteBlocks(processedBlockInfos.SelectMany(b => b.DeletedBlockIds));

            var nonEmptyBlocks = processedBlockInfos
                .Where(bi => bi.DataBlockBuilder == null
                || ((IBlock)bi.DataBlockBuilder).RecordCount > 0);

            if (metaMetaBlockId != null)
            {
                throw new NotImplementedException();
            }
            else
            {
                tx.LoadCommittedBlocksInTransaction(metadataTableName);

                var metaBlockBuilder = tx.TransactionState.UncommittedTransactionLog
                    .TransactionTableLogMap[metadataTableName]
                    .CommittedDataBlock!;

                //  Delete blocks in-memory
                metaBlockBuilder.DeleteRecordsByRecordIndex(
                    Enumerable.Range(0, ((IBlock)metaBlockBuilder).RecordCount));
                if (nonEmptyBlocks.Any())
                {
                    throw new NotImplementedException();
                }
            }

            return processedBlockInfos
                .Where(bi => bi.DataBlockBuilder != null)
                .Any();
        }

        #region Hard Deletion
        private void HardDeleteRecords(
            string tableName,
            IEnumerable<long> recordIds,
            TransactionContext tx)
        {
            if (recordIds.Any())
            {
                var tombstoneTable = Database.TombstoneTable;
                var tombstoneTableName = tombstoneTable.Schema.TableName;

                tx.LoadCommittedBlocksInTransaction(tombstoneTableName);

                var builder = tx.TransactionState
                    .UncommittedTransactionLog
                    .TransactionTableLogMap[tombstoneTableName]
                    .CommittedDataBlock!;
                var predicate = tombstoneTable.Query()
                    .Where(pf => pf.Equal(t => t.TableName, tableName))
                    .Where(pf => pf.In(t => t.DeletedRecordId, recordIds))
                    .Predicate;
                var rowIndexes = ((IBlock)builder).Filter(predicate, false)
                    .RowIndexes;

                builder.DeleteRecordsByRecordIndex(rowIndexes);
            }
        }

        private void DeleteBlocks(IEnumerable<int> blockIds)
        {
            Database.ChangeDatabaseState(state => state with
            {
                DiscardedBlockIds = state.DiscardedBlockIds.AddRange(blockIds)
            });
        }

        private IImmutableList<long> GetTombstonedRecords(
            string tableName,
            long minRecordId,
            long maxRecordId,
            TransactionContext tx)
        {
            tx.LoadCommittedBlocksInTransaction(Database.TombstoneTable.Schema.TableName);

            var recordIds = Database.TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.GreaterThanOrEqual(t => t.DeletedRecordId, minRecordId))
                .Where(pf => pf.LessThanOrEqual(t => t.DeletedRecordId, maxRecordId))
                .Select(t => t.DeletedRecordId)
                .ToImmutableArray();

            return recordIds;
        }
        #endregion

        #region Find blocks
        private int? FindDataBlock(string tableName, long deletedRecordId, TransactionContext tx)
        {
            var table = Database.GetAnyTable(tableName);
            var predicate = new BinaryOperatorPredicate(
                table.Schema.RecordIdColumnIndex,
                deletedRecordId,
                BinaryOperator.Equal);
            var blockId = table.Query(tx)
                .WithIgnoreDeleted()
                .WithPredicate(predicate)
                .WithProjection([table.Schema.ParentBlockIdColumnIndex])
                .Select(r => (int?)r.Span[0])
                .FirstOrDefault();

            return blockId;
        }

        private int? FindParentBlock(
            string metadataTableName,
            int blockId,
            TransactionContext tx)
        {
            var metadataTable = Database.GetAnyTable(metadataTableName);
            var predicate = new BinaryOperatorPredicate(
                ((MetadataTableSchema)metadataTable.Schema).BlockIdColumnIndex,
                blockId,
                BinaryOperator.Equal);
            var parentBlockId = metadataTable.Query(tx)
                .WithPredicate(predicate)
                .WithProjection([metadataTable.Schema.ParentBlockIdColumnIndex])
                .Select(r => (int)r.Span[0]!)
                .FirstOrDefault();

            return parentBlockId > 0 ? parentBlockId : null;
        }
        #endregion

        #region Load block infos
        private IImmutableList<BlockInfo> LoadBlockInfos(
            string metadataTableName,
            int? metaBlockId,
            TransactionContext tx)
        {
            if (metaBlockId != null)
            {
                throw new NotImplementedException();
            }
            else
            {   //  We merge blocks in memory
                tx.LoadCommittedBlocksInTransaction(metadataTableName);

                var blockInfos = tx.TransactionState.InMemoryDatabase
                    .TransactionTableLogsMap[metadataTableName]
                    .InMemoryBlocks
                    .SelectMany(b => LoadBlockInfosFromMetaBlock(b))
                    .Select(b => b)
                    .ToImmutableArray();

                return blockInfos;
            }
        }

        private IEnumerable<BlockInfo> LoadBlockInfosFromMetaBlock(IBlock metaBlock)
        {
            return metaBlock.Project(
                new object?[metaBlock.TableSchema.Columns.Count],
                Enumerable.Range(0, metaBlock.TableSchema.Columns.Count)
                .ToImmutableArray(),
                Enumerable.Range(0, metaBlock.RecordCount),
                0)
                .Select(r => BlockInfo.FromMetadataBlock(
                    new MetaDataBlock(r.ToArray(), (MetadataTableSchema)metaBlock.TableSchema)));
        }
        #endregion
    }
}