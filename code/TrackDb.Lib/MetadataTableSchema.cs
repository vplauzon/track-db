using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block;

namespace TrackDb.Lib
{
    internal class MetadataTableSchema : TableSchema
    {
        private const string ITEM_COUNT = "$itemCount";
        private const string SIZE = "$size";
        private const string BLOCK_ID = "$blockId";

        private readonly TableSchema _parentSchema;

        #region Constructor
        public static MetadataTableSchema FromParentSchema(TableSchema parentSchema)
        {
            var dataColumns = parentSchema.ColumnProperties
                .Where(c => c.ColumnSchema.IsIndexed && c.ColumnSchemaStat != ColumnSchemaStat.Data)
                //  For each column we create a min, max & hasNulls column
                .Select(c => new[]
                {
                    new ColumnSchemaProperties(
                        new ColumnSchema($"$min-{c.ColumnSchema.ColumnName}", c.ColumnSchema.ColumnType),
                        ColumnSchemaStat.Min),
                    new ColumnSchemaProperties(
                        new ColumnSchema($"$max-{c.ColumnSchema.ColumnName}", c.ColumnSchema.ColumnType),
                        ColumnSchemaStat.Max)
                })
                //  We add the record-id columns
                .Append(
                [
                    new ColumnSchemaProperties(
                        new ColumnSchema("$min-$recordId", typeof(long)),
                        ColumnSchemaStat.Min),
                    new ColumnSchemaProperties(
                        new ColumnSchema("$max-$recordId", typeof(long)),
                        ColumnSchemaStat.Max)
                ])
                .SelectMany(c => c);
            var inheritedMetaDataColumns = parentSchema.ColumnProperties
                .Where(c => c.ColumnSchema.IsIndexed && c.ColumnSchemaStat != ColumnSchemaStat.Data);
            var metaDataColumns = dataColumns
                .Concat(inheritedMetaDataColumns)
                //  We add a few standard meta data columns
                .Concat([
                    new ColumnSchemaProperties(
                        new ColumnSchema(ITEM_COUNT, typeof(int), false),
                        ColumnSchemaStat.Data),
                    new ColumnSchemaProperties(
                        new ColumnSchema(SIZE, typeof(int), false),
                        ColumnSchemaStat.Data),
                    new ColumnSchemaProperties(
                        new ColumnSchema(BLOCK_ID, typeof(int), false),
                        ColumnSchemaStat.Data)
                    ]);

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
            _parentSchema = parentSchema;
        }
        #endregion

        #region Metadata Columns
        public int ItemCountColumnIndex => Columns.Count - 3;

        public int SizeColumnIndex => Columns.Count - 2;

        public int BlockIdColumnIndex => Columns.Count - 1;
        #endregion

        public ReadOnlySpan<object?> CreateMetadataRecord(
            int itemCount,
            int size,
            int blockId,
            IEnumerable<object?> columnMinima,
            IEnumerable<object?> columnMaxima)
        {
            var metaData = columnMinima
                .Zip(columnMaxima)
                .Select(o => new object?[]
                {
                    o.First,
                    o.Second
                })
                .SelectMany(c => c)
                .Append(itemCount)
                .Append(size)
                .Append(blockId)
                .ToArray();

            return metaData;
        }
    }
}