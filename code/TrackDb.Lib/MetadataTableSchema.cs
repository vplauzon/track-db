using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal class MetadataTableSchema : TableSchema
    {
        private const string RECORD_ID = "$recordId";
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
                            new ColumnSchemaProperties(
                                new ColumnSchema($"$min-{schema.ColumnName}", schema.ColumnType),
                                ColumnSchemaStat.Min,
                                parentColumn.ColumnSchema),
                            new ColumnSchemaProperties(
                                new ColumnSchema($"$max-{schema.ColumnName}", schema.ColumnType),
                                ColumnSchemaStat.Max,
                                parentColumn.ColumnSchema)];
                    case ColumnSchemaStat.Min:
                        return [parentColumn];
                    case ColumnSchemaStat.Max:
                        return [parentColumn];

                    default:
                        throw new NotSupportedException(
                            $"{nameof(ColumnSchemaStat)}.{parentColumn.ColumnSchemaStat}");
                }
            }
            var isParentMeta = parentSchema is MetadataTableSchema;
            var existingColumns = isParentMeta
                ? parentSchema.ColumnProperties
                : parentSchema.ColumnProperties
                //  Add record ID
                .Append(new(new(RECORD_ID, typeof(long)), ColumnSchemaStat.Data));
            var inheritedMetaDataColumns = existingColumns
                .Where(c => c.ColumnSchema.IsIndexed)
                .Select(c => CreateMetaColumn(c));
            var newDataColumns = new ColumnSchemaProperties[]
            {
                new ColumnSchemaProperties(
                    new ColumnSchema(ITEM_COUNT, typeof(int), false),
                    ColumnSchemaStat.Data),
                new ColumnSchemaProperties(
                    new ColumnSchema(SIZE, typeof(int), false),
                    ColumnSchemaStat.Data),
                new ColumnSchemaProperties(
                    new ColumnSchema(BLOCK_ID, typeof(int), true),
                    ColumnSchemaStat.Data)
            };
            var metaDataColumns = inheritedMetaDataColumns
                .Append(newDataColumns)
                .SelectMany(c => c);

            return new(
                parentSchema,
                $"$meta-{parentSchema.TableName}",
                metaDataColumns);
        }

        private MetadataTableSchema(
            TableSchema parentSchema,
            string tableName,
            IEnumerable<ColumnSchemaProperties> columnProperties)
            : base(tableName, columnProperties, ImmutableArray<int>.Empty, ImmutableArray<int>.Empty)
        {
            ParentSchema = parentSchema;
        }
        #endregion

        public TableSchema ParentSchema { get; }

        #region Metadata Columns
        public int ItemCountColumnIndex => Columns.Count - 3;

        public int SizeColumnIndex => Columns.Count - 2;

        public int BlockIdColumnIndex => Columns.Count - 1;
        #endregion

        #region Stats Column
        public int RecordIdMinColumnIndex => ColumnProperties
            .Index()
            .Where(c => c.Item.ColumnSchemaStat == ColumnSchemaStat.Min)
            .Where(c => c.Item.ParentColumnSchema!.Value.ColumnName == RECORD_ID)
            .Select(c => c.Index)
            .First();

        public int RecordIdMaxColumnIndex => ColumnProperties
            .Index()
            .Where(c => c.Item.ColumnSchemaStat == ColumnSchemaStat.Max)
            .Where(c => c.Item.ParentColumnSchema!.Value.ColumnName == RECORD_ID)
            .Select(c => c.Index)
            .First();
        #endregion

        /// <summary>
        /// Create a metadata record from statistics about records from the parent schema.
        /// </summary>
        /// <param name="itemCount"></param>
        /// <param name="size"></param>
        /// <param name="blockId"></param>
        /// <param name="columnMinima"></param>
        /// <param name="columnMaxima"></param>
        /// <returns></returns>
        public ReadOnlySpan<object?> CreateMetadataRecord(
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

            var columnPropertiesWithRecordId = ParentSchema.ColumnProperties
                .Append(new(new("creationTime", typeof(DateTime), false), ColumnSchemaStat.Data))
                .Append(new(new("$recordId", typeof(long), true), ColumnSchemaStat.Data));
            var stats = columnMinima
                .Zip(columnMaxima, columnPropertiesWithRecordId)
                .Select(b => new
                {
                    Properties = b.Third,
                    Minimum = b.First,
                    Maximum = b.Second
                })
                //  We discard columns that are not indexed
                .Where(o => o.Properties.ColumnSchema.IsIndexed)
                .Select(o => CreateMetaColumnValues(o.Properties, o.Minimum, o.Maximum))
                .SelectMany(c => c);
            var statsWithExtraColumns = stats
                .Append(itemCount)
                .Append(size)
                .Append(blockId);
            var record = statsWithExtraColumns.ToArray();

            return record;
        }
    }
}