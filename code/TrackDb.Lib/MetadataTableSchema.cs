using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib
{
    /// <summary>Represents a table holding metadata.</summary>
    internal sealed class MetadataTableSchema : TableSchema
    {
        private const string ITEM_COUNT = "$itemCount";
        private const string SIZE = "$size";
        private const string BLOCK_ID = "$blockId";

        #region Constructor
        public static MetadataTableSchema FromParentSchema(TableSchema parentSchema)
        {
            ColumnSchemaProperties[] CreateMetaColumn(ColumnSchemaProperties parentColumn)
            {
                var schema = parentColumn.ColumnSchema;

                switch (parentColumn.ColumnSchemaStat)
                {
                    case ColumnSchemaStat.Data:
                        return [
                            ColumnSchemaProperties.CreateGenerationOne(parentColumn, ColumnSchemaStat.Min),
                            ColumnSchemaProperties.CreateGenerationOne(parentColumn, ColumnSchemaStat.Max)];
                    case ColumnSchemaStat.Min:
                    case ColumnSchemaStat.Max:
                        return [ColumnSchemaProperties.CreateGenerationTwo(parentColumn)];

                    default:
                        throw new NotSupportedException(
                            $"{nameof(ColumnSchemaStat)}.{parentColumn.ColumnSchemaStat}");
                }
            }
            var tableName = $"$meta-{parentSchema.TableName}";
            var inheritedMetaDataColumns = parentSchema.ColumnProperties
                .Where(c => c.ColumnSchema.IsIndexed)
                .SelectMany(c => CreateMetaColumn(c));
            var extraColumns = new ColumnSchemaProperties[]
            {
                ColumnSchemaProperties.CreateGenerationZero(
                    new ColumnSchema(ITEM_COUNT, typeof(int), false)),
                ColumnSchemaProperties.CreateGenerationZero(
                    new ColumnSchema(SIZE, typeof(int), false)),
                ColumnSchemaProperties.CreateGenerationZero(
                    new ColumnSchema($"{BLOCK_ID}-{tableName}", typeof(int), true))
            };

            return new(
                parentSchema,
                tableName,
                inheritedMetaDataColumns
                .Concat(extraColumns),
                Array.Empty<ColumnSchemaProperties>());
        }

        private MetadataTableSchema(
            TableSchema parentSchema,
            string tableName,
            IEnumerable<ColumnSchemaProperties> columnProperties,
            IEnumerable<ColumnSchemaProperties> extraColumnProperties)
            : base(
                  tableName,
                  columnProperties,
                  extraColumnProperties,
                  ImmutableArray<int>.Empty,
                  ImmutableArray<int>.Empty,
                  ImmutableArray<TableTriggerAction>.Empty)
        {
            ParentSchema = parentSchema;
            RecordIdMinColumnIndex = ColumnProperties
                .Index()
                .Where(c => c.Item.ColumnSchemaStat == ColumnSchemaStat.Min)
                .Where(c => c.Item.AncestorZeroColumnName == DataTableSchema.RECORD_ID)
                .Select(c => c.Index)
                .First();
            RecordIdMaxColumnIndex = ColumnProperties
                .Index()
                .Where(c => c.Item.ColumnSchemaStat == ColumnSchemaStat.Max)
                .Where(c => c.Item.AncestorZeroColumnName == DataTableSchema.RECORD_ID)
                .Select(c => c.Index)
                .First();

            ItemCountColumnIndex = ColumnProperties.Count - 3;
            SizeColumnIndex = ColumnProperties.Count - 2;
            BlockIdColumnIndex = ColumnProperties.Count - 1;
        }
        #endregion

        public TableSchema ParentSchema { get; }

        #region Metadata Columns
        public int ItemCountColumnIndex { get; }

        public int SizeColumnIndex { get; }

        public int BlockIdColumnIndex { get; }
        #endregion

        #region Stats Column
        public int RecordIdMinColumnIndex { get; }

        public int RecordIdMaxColumnIndex { get; }
        #endregion

        internal override bool IsMetadata => true;

        /// <summary>
        /// Create a metadata record from statistics about records from the parent schema.
        /// </summary>
        /// <param name="blockId"></param>
        /// <param name="blockStats"></param>
        /// <returns></returns>
        public ReadOnlyMemory<object?> CreateMetadataRecord(int blockId, BlockStats blockStats)
        {
            return CreateMetadataRecord(
                blockStats.ItemCount,
                blockStats.Size,
                blockId,
                blockStats.Columns.Select(c => c.ColumnMinimum),
                blockStats.Columns.Select(c => c.ColumnMaximum));
        }

        public IEnumerable<MetadataColumnCorrespondance> GetColumnCorrespondances()
        {
            var metaColumnsByColumnName = ColumnProperties
                .Index()
                .Where(p => p.Item.Generation > 0)
                .GroupBy(p => new
                {
                    p.Item.AncestorZeroColumnName,
                    p.Item.Generation
                })
                .ToImmutableDictionary(g => g.Key);
            var parentColumnProperties = ParentSchema.ColumnProperties;
            var correspondances = new List<MetadataColumnCorrespondance>();

            for (var i = 0; i != parentColumnProperties.Count; ++i)
            {
                if (metaColumnsByColumnName.TryGetValue(
                    //  We want to find the children of the current parent column
                    new
                    {
                        parentColumnProperties[i].AncestorZeroColumnName,
                        Generation = parentColumnProperties[i].Generation + 1
                    },
                    out var metaColumns))
                {
                    if (metaColumns.Count() == 2)
                    {
                        correspondances.Add(new MetadataColumnCorrespondance(
                            i,
                            parentColumnProperties[i].ColumnSchema,
                            metaColumns
                            .Where(c => c.Item.ColumnSchemaStat == ColumnSchemaStat.Min)
                            .First()
                            .Index,
                            metaColumns
                            .Where(c => c.Item.ColumnSchemaStat == ColumnSchemaStat.Max)
                            .First()
                            .Index));
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            $"Meta columns count is {metaColumns.Count()}");
                    }
                }
            }

            return correspondances;
        }

        private ReadOnlyMemory<object?> CreateMetadataRecord(
            int itemCount,
            int size,
            int blockId,
            IEnumerable<object?> columnMinima,
            IEnumerable<object?> columnMaxima)
        {
            object?[] CreateMetaColumnValues(
                ColumnSchemaProperties properties,
                object? minimum,
                object? maximum)
            {
                switch (properties.ColumnSchemaStat)
                {
                    case ColumnSchemaStat.Data:
                        return [minimum, maximum];
                    case ColumnSchemaStat.Min:
                        //  We take the minimum of the minima
                        return [minimum];
                    case ColumnSchemaStat.Max:
                        //  We take the maximum of the maxima
                        return [maximum];

                    default:
                        throw new NotSupportedException(
                            $"{nameof(ColumnSchemaStat)}.{properties.ColumnSchemaStat}");
                }
            }

            var indexedColumns = columnMinima
                .Zip(columnMaxima, ParentSchema.ColumnProperties)
                .Select(b => new
                {
                    Properties = b.Third,
                    Minimum = b.First,
                    Maximum = b.Second
                })
                //  We discard columns that are not indexed
                .Where(o => o.Properties.ColumnSchema.IsIndexed);
            var stats = indexedColumns
                .Select(o => CreateMetaColumnValues(o.Properties, o.Minimum, o.Maximum))
                .SelectMany(c => c);
            var statsWithExtraColumns = stats
                .Append(itemCount)
                .Append(size)
                .Append(blockId);
            var record = statsWithExtraColumns.ToArray();

            #region DEBUG
            if (record.Length != ColumnProperties.Count)
            {
                throw new InvalidOperationException(
                    $"Meta schema should be {ColumnProperties.Count} but is {record.Length} columns");
            }
            #endregion

            return record;
        }
    }
}