using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.DataLifeCycle.Persistance
{
    /// <summary>
    /// Encapsulation of data management around metablocks.
    /// Everything is done on committed data (no uncommitted data).
    /// </summary>
    internal partial class MetaBlockManager : LogicBase
    {
        #region Inner types
        private record ExtremaRecordId(long Min, long Max)
        {
            public static ExtremaRecordId Seed { get; } =
                new ExtremaRecordId(long.MaxValue, long.MinValue);

            public static ExtremaRecordId ComputeExtrema(IEnumerable<long> recordIds)
            {
                var aggregate = recordIds
                    .Aggregate(new ExtremaRecordId(0, 0), (aggregate, recordId) => new ExtremaRecordId(
                        Math.Min(aggregate.Min, recordId),
                        Math.Max(aggregate.Max, recordId)));

                return aggregate;
            }

            public static ExtremaRecordId ComputeExtrema(
                IEnumerable<(long RecordIdMin, long RecordIdMax)> extrema)
            {
                var aggregate = extrema
                    .Aggregate(Seed, (aggregate, extremum) => new ExtremaRecordId(
                        Math.Min(aggregate.Min, extremum.RecordIdMin),
                        Math.Max(aggregate.Max, extremum.RecordIdMax)));

                return aggregate;
            }
        }
        #endregion

        public MetaBlockManager(Database database, TransactionContext tx)
            : base(database)
        {
            Tx = tx;
        }

        public TransactionContext Tx { get; }

        #region ListMetaMetaBlocks
        public IEnumerable<MetaMetaBlockStat> ListMetaMetaBlocks(string tableName)
        {
            var nonNullMetaMetaBlocks = ListNonNullMetaMetaBlocks(tableName);
            var nullMetaMetaBlocks = ListNullMetaMetaBlock(tableName);
            var metaMetaBlocks = nonNullMetaMetaBlocks.Concat(nullMetaMetaBlocks);
            var tombstoneExtrema = GetTombstoneRecordIdExtrema(tableName);

            foreach (var m in metaMetaBlocks)
            {   //  Check if the meta block might have tombstone records
                //  This is to avoid doing a tombstone query for each meta block
                //  We test for intersection
                if (tombstoneExtrema.Max >= m.MinRecordId
                    && m.MaxRecordId >= tombstoneExtrema.Min)
                {
                    var recordCount = Database.TombstoneTable.Query(Tx)
                        .WithCommittedOnly()
                        .Where(pf => pf.Equal(t => t.TableName, tableName))
                        .Where(pf => pf.GreaterThanOrEqual(t => t.DeletedRecordId, m.MinRecordId)
                        .And(pf.LessThanOrEqual(t => t.DeletedRecordId, m.MaxRecordId)))
                        .Count();
                    var blockStat = m with
                    {
                        TombstonedRecordCount = recordCount
                    };

                    yield return blockStat;
                }
            }
        }

        private IEnumerable<MetaMetaBlockStat> ListNonNullMetaMetaBlocks(string tableName)
        {
            var metaTable = Database.GetMetaDataTable(tableName);
            var metaMetaTable = Database.GetMetaDataTable(metaTable.Schema.TableName);
            var metaMetaSchema = (MetadataTableSchema)metaMetaTable.Schema;
            //  In theory, we could have multiple in-memory (i.e. non positive) meta-meta block
            //  In practice, we'll only have one since it will sit in the committed part
            //  of the transaction in one BlockBuilder
            var metaMetaRecords = metaMetaTable.Query(Tx)
                .WithCommittedOnly()
                .WithProjection(
                metaMetaSchema.BlockIdColumnIndex,
                metaMetaSchema.RecordIdMinColumnIndex,
                metaMetaSchema.RecordIdMaxColumnIndex)
                .Select(r => new
                {
                    BlockId = (int?)r.Span[0],
                    MinRecordId = (long)r.Span[1]!,
                    MaxRecordId = (long)r.Span[2]!
                })
                .Select(o => new MetaMetaBlockStat(o.BlockId, o.MinRecordId, o.MaxRecordId, 0));

            return metaMetaRecords;
        }

        private IEnumerable<MetaMetaBlockStat> ListNullMetaMetaBlock(string tableName)
        {
            var metaTable = Database.GetMetaDataTable(tableName);
            var metaSchema = (MetadataTableSchema)metaTable.Schema;
            //  Take only the blocks in-memory:  that is the "null blockId"
            var extrema = metaTable.Query(Tx)
                .WithCommittedOnly()
                .WithInMemoryOnly()
                .WithProjection(metaSchema.RecordIdMinColumnIndex, metaSchema.RecordIdMaxColumnIndex)
                .Select(r => ((long)r.Span[0]!, (long)r.Span[1]!));
            var metaExtremum = ExtremaRecordId.ComputeExtrema(extrema);

            if (metaExtremum == ExtremaRecordId.Seed)
            {
                return Array.Empty<MetaMetaBlockStat>();
            }
            else
            {
                return [new MetaMetaBlockStat(null, metaExtremum.Min, metaExtremum.Max, 0)];
            }
        }

        private ExtremaRecordId GetTombstoneRecordIdExtrema(string tableName)
        {
            var query = Database.TombstoneTable.Query(Tx)
                .WithCommittedOnly()
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .TableQuery
                .WithProjection(Database.TombstoneTable.Schema.GetColumnIndexSubset(
                    t => t.DeletedRecordId))
                .Select(r => (long)r.Span[0]!);
            var aggregate = ExtremaRecordId.ComputeExtrema(query);

            return aggregate;
        }
        #endregion

        #region Load Blocks
        /// <summary>
        /// Load blocks for table <paramref name="tableName"/> that belong to the meta block
        /// <paramref name="metaBlockId"/>.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="metaBlockId"></param>
        /// <returns></returns>
        public IEnumerable<MetadataBlock> LoadBlocks(string tableName, int? metaBlockId)
        {
            if (metaBlockId != null && metaBlockId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(metaBlockId));
            }

            Table metaDataTable = Database.GetMetaDataTable(tableName);
            Table metaMetaDataTable = Database.GetMetaDataTable(metaDataTable.Schema.TableName);
            var metadataTableSchema = (MetadataTableSchema)metaDataTable.Schema;
            var columnIndexes = Enumerable.Range(0, metadataTableSchema.Columns.Count)
                .ToImmutableArray();

            IEnumerable<ReadOnlyMemory<object?>> LoadNullBlocks(string tableName)
            {
                var results = metaDataTable.Query(Tx)
                    //  Especially relevant for availability-block:
                    //  We just want to deal with what is committed
                    .WithCommittedOnly()
                    .WithInMemoryOnly()
                    .WithProjection(columnIndexes);

                return results;
            }

            IEnumerable<ReadOnlyMemory<object?>> LoadPositiveBlocks(
                string tableName,
                int metaBlockId)
            {
                var metaMetaBlock = Database.GetOrLoadBlock(metaBlockId, metaDataTable.Schema);
                var results = metaMetaBlock.Project(
                    new object?[columnIndexes.Length],
                    columnIndexes,
                    Enumerable.Range(0, metaMetaBlock.RecordCount),
                    0);

                return results;
            }

            var results = metaBlockId == null
                    ? LoadNullBlocks(tableName)
                    : LoadPositiveBlocks(tableName, metaBlockId.Value);
            var blocks = results
                .Select(r => new MetadataBlock(r.ToArray(), metadataTableSchema))
                .ToImmutableArray();

            return blocks;
        }
        #endregion

        public void ReplaceInMemoryBlocks(string metaTableName, BlockBuilder metaBuilder)
        {
            Tx.LoadCommittedBlocksInTransaction(metaTableName);

            var committedDataBlock = Tx.TransactionState
                .UncommittedTransactionLog
                .TransactionTableLogMap[metaTableName]
                .CommittedDataBlock!;

            committedDataBlock.DeleteAll();
            committedDataBlock.AppendBlock(metaBuilder);
            PruneHeadMetadata(metaTableName);
        }

        public int? GetMetaBlockId(string metaTableName, int blockId)
        {
            if (blockId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(blockId));
            }
            else
            {
                var metaTable = Database.GetAnyTable(metaTableName);
                var metaSchema = (MetadataTableSchema)metaTable.Schema;
                var parentBlockIds = metaTable.Query(Tx)
                    .WithCommittedOnly()
                    .WithPredicate(new BinaryOperatorPredicate(
                        metaSchema.BlockIdColumnIndex,
                        blockId,
                        BinaryOperator.Equal))
                    .WithProjection(metaSchema.ParentBlockIdColumnIndex)
                    .Take(1)
                    .Select(r => (int?)r.Span[0]!)
                    .ToImmutableArray();

                if (parentBlockIds.Length == 0)
                {
                    throw new InvalidOperationException(
                        $"Can't find block ID '{blockId}' in table {metaTableName}");
                }

                var metaBlockId = parentBlockIds[0];

                return metaBlockId > 0 ? metaBlockId : null;
            }
        }

        private void PruneHeadMetadata(string metaTableName)
        {
            Tx.LoadCommittedBlocksInTransaction(metaTableName);

            var metaBlock = Tx.TransactionState
                .UncommittedTransactionLog
                .TransactionTableLogMap[metaTableName]
                .CommittedDataBlock;

            //  We want a single block on the head
            if (metaBlock != null && ((IBlock)metaBlock).RecordCount == 1)
            {
                var metaTable = Database.GetAnyTable(metaTableName);
                var metaSchema = (MetadataTableSchema)metaTable.Schema;

                //  We want the table underneath to be a meta table
                if (metaSchema.ParentSchema is MetadataTableSchema schema)
                {
                    var tableInMemoryCount = Database.GetAnyTable(schema.TableName).Query(Tx)
                        .WithInMemoryOnly()
                        .Count();

                    //  We want the table to have only one block from the meta block / none in-memory
                    if (tableInMemoryCount == 0)
                    {
                        var metaRecord = ((IBlock)metaBlock).Project(
                            new object?[2],
                            [schema.BlockIdColumnIndex, schema.ItemCountColumnIndex],
                            [0],
                            0)
                            .Select(r => new
                            {
                                BlockId = (int)r.Span[0]!,
                                ItemCount = (int)r.Span[1]!
                            })
                            .First();
                        var block = Database.GetOrLoadBlock(metaRecord.BlockId, schema);

                        //  We remove the metablock
                        Database.SetNoLongerInUsedBlockIds([metaRecord.BlockId], Tx);
                        metaBlock.DeleteAll();
                        //  We promote the block listed in the metablock in-memory
                        Tx.LoadCommittedBlocksInTransaction(schema.TableName);

                        var committedDataBlock = Tx.TransactionState
                            .UncommittedTransactionLog
                            .TransactionTableLogMap[schema.TableName]
                            .CommittedDataBlock!;

                        committedDataBlock.AppendBlock(block);
                        PruneHeadMetadata(schema.TableName);
                    }
                }
            }
        }
    }
}