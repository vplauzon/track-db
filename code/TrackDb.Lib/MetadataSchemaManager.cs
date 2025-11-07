using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TrackDb.Lib
{
    internal class MetadataSchemaManager
    {
        private const string ITEM_COUNT = "$itemCount";
        private const string SIZE = "$size";
        private const string BLOCK_ID = "$blockId";

        #region Constructors
        public static MetadataSchemaManager FromTableSchema(TableSchema tableSchema)
        {
            var metaDataColumns = tableSchema.Columns
                //  For each column we create a min, max & hasNulls column
                .Select(c => new[]
                {
                    new ColumnSchema($"$min-{c.ColumnName}", c.ColumnType),
                    new ColumnSchema($"$max-{c.ColumnName}", c.ColumnType)
                })
                //  We add the record-id columns
                .Append(
                [
                    new ColumnSchema("$min-$recordId", typeof(long)),
                    new ColumnSchema("$max-$recordId", typeof(long))
                ])
                //  We add the itemCount & block-id columns
                .Append(
                [
                    new ColumnSchema(ITEM_COUNT, typeof(int)),
                    new ColumnSchema(SIZE, typeof(int)),
                    new ColumnSchema(BLOCK_ID, typeof(int))
                ])
                //  We fan out the columns
                .SelectMany(c => c);
            var metadataSchema = new TableSchema(
                $"$meta-{tableSchema.TableName}",
                metaDataColumns,
                Array.Empty<int>(),
                Array.Empty<int>());

            return new(metadataSchema);
        }

        public static MetadataSchemaManager FromMetadataTableSchema(TableSchema metadataSchema)
        {
            return new(metadataSchema);
        }

        private MetadataSchemaManager(TableSchema metadataSchema)
        {
            MetadataSchema = metadataSchema;
        }
        #endregion

        public TableSchema MetadataSchema { get; }

        #region Metadata Columns
        public int ItemCountColumnIndex => MetadataSchema.Columns.Count - 3;

        public int SizeColumnIndex => MetadataSchema.Columns.Count - 2;

        public int BlockIdColumnIndex => MetadataSchema.Columns.Count - 1;
        #endregion
    }
}