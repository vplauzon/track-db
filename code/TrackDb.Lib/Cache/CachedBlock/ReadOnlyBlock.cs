using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Cache.CachedBlock
{
    internal class ReadOnlyBlock : ReadOnlyBlockBase
    {
        #region Constructors
        public ReadOnlyBlock(TableSchema schema, SerializedBlock serializedBlock)
            : base(schema, CreateColumns(schema, serializedBlock))
        {
        }

        private static IEnumerable<IReadOnlyDataColumn> CreateColumns(
            TableSchema schema,
            SerializedBlock serializedBlock)
        {
            var serializedColumns = serializedBlock.CreateSerializedColumns();
            var columns = schema.Columns
                .Select(c => c.ColumnType)
                //  Append the Record ID column
                .Append(typeof(long))
                .Zip(serializedColumns, (ColumnType, SerializedColumn) => new
                {
                    ColumnType,
                    SerializedColumn
                })
                .Select(o => CreateColumn(o.ColumnType, o.SerializedColumn));

            return columns;
        }

        private static IReadOnlyDataColumn CreateColumn(
            Type columnType,
            SerializedColumn serializedColumn)
        {
            if (DataColumnFactories.TryGetValue(columnType, out var columnFactory))
            {
                return new JitReadOnlyDataColumn(columnFactory, serializedColumn);
            }
            else
            {
                throw new ArgumentException(
                    $"Column type '{columnType.Name}' isn't supported",
                    nameof(columnType));
            }
        }
        #endregion
    }
}