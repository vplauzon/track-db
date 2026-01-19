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
    /// <remarks>
    /// The semantic of the blockId field is as follow:
    /// <list type="bullet">
    /// <item>Positive numbers:  an actual meta block ID persisted on disk.</item>
    /// <item>
    /// Non-positive number (including zero):  a meta block living in the in-memory
    /// part of the meta-meta table.
    /// </item>
    /// <item>
    /// Null:  blocks are living in the meta table with no meta meta block (the
    /// meta meta table might not event exist).
    /// </item>
    /// </list>
    /// </remarks>
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
        public IEnumerable<MetadataBlock> LoadBlocks(string tableName, int? metaBlockId)
        {
            var metadataTable = Database.GetMetaDataTable(tableName);
            var metaMetadataTable = Database.GetMetaDataTable(metadataTable.Schema.TableName);
            var metadataTableSchema = (MetadataTableSchema)metadataTable.Schema;
            var columnIndexes = Enumerable.Range(0, metadataTableSchema.Columns.Count)
                .ToImmutableArray();

            IEnumerable<ReadOnlyMemory<object?>> LoadNullBlocks(string tableName)
            {
                var results = metadataTable.Query(Tx)
                    //  Especially relevant for availability-block:
                    //  We just want to deal with what is committed
                    .WithCommittedOnly()
                    .WithInMemoryOnly()
                    .WithProjection(columnIndexes);

                return results;
            }

            IEnumerable<ReadOnlyMemory<object?>> LoadZeroBlocks(string tableName)
            {
                var results = metadataTable.Query(Tx)
                    .WithCommittedOnly()
                    .WithInMemoryOnly()
                    //  Especially relevant for availability-block:
                    //  We just want to deal with what is committed
                    .WithProjection(columnIndexes);

                return results;
            }

            IEnumerable<ReadOnlyMemory<object?>> LoadPositiveBlocks(
                string tableName,
                int metaBlockId)
            {
                var metaMetaBlock = Database.GetOrLoadBlock(metaBlockId, metadataTable.Schema);
                var results = metaMetaBlock.Project(
                    new object?[columnIndexes.Length],
                    columnIndexes,
                    Enumerable.Range(0, metaMetaBlock.RecordCount),
                    0);

                return results;
            }

            var results = metaBlockId == null
                    ? LoadNullBlocks(tableName)
                    : metaBlockId == 0
                    ? LoadZeroBlocks(tableName)
                    : LoadPositiveBlocks(tableName, metaBlockId.Value);
            var blocks = results
                .Select(r => new MetadataBlock(r.ToArray(), metadataTableSchema))
                .ToImmutableArray();

            return blocks;
        }
        #endregion

        public void ReplaceInMemoryBlocks(
            string metaTableName,
            BlockBuilder metaBuilder)
        {
            Tx.LoadCommittedBlocksInTransaction(metaTableName);

            var committedDataBlock = Tx.TransactionState
                .UncommittedTransactionLog
                .TransactionTableLogMap[metaTableName]
                .CommittedDataBlock!;

            committedDataBlock.DeleteAll();
            committedDataBlock.AppendBlock(metaBuilder);
        }

        public int? GetMetaBlockId(string metaTableName, int blockId)
        {
            if (blockId == 0)
            {
                return null;
            }
            else
            {
                var table = Database.GetAnyTable(metaTableName);
                var metaSchema = (MetadataTableSchema)table.Schema;
                var parentBlockIds = table.Query(Tx)
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

                return parentBlockIds[0];
            }
        }
    }
}