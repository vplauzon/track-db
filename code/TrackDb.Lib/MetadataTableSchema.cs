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
            var metaDataColumns = parentSchema.Columns
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

            return new(
                parentSchema,
                $"$meta-{parentSchema.TableName}",
                metaDataColumns.ToImmutableArray());
        }

        private MetadataTableSchema(
            TableSchema parentSchema,
            string tableName,
            IImmutableList<ColumnSchema> columns)
            : base(tableName, columns, ImmutableArray<int>.Empty, ImmutableArray<int>.Empty)
        {
            _parentSchema = parentSchema;
        }
        #endregion

        //internal override MetadataTableSchema CreateMetadataTableSchema()
        //{
        //    throw new NotImplementedException();
        //}

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