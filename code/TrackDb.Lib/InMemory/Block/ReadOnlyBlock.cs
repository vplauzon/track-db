using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.InMemory.Block
{
    internal class ReadOnlyBlock : ReadOnlyBlockBase
    {
        private readonly IImmutableList<Lazy<IReadOnlyDataColumn>> _dataColumns;
        private readonly int _recordCount;

        #region Constructors
        public ReadOnlyBlock(TableSchema schema, SerializedBlock serializedBlock)
            : base(schema)
        {
            var serializedColumns = serializedBlock.CreateSerializedColumns();

            _dataColumns = schema.Columns
                .Select(c => c.ColumnType)
                //  Append the Record ID column
                .Append(typeof(long))
                .Zip(serializedColumns, (ColumnType, SerializedColumn) => new
                {
                    ColumnType,
                    SerializedColumn
                })
                .Select(o => CreateColumn(o.ColumnType, o.SerializedColumn))
                .ToImmutableArray();
            _recordCount = serializedBlock.MetaData.ItemCount;
        }

        private static Lazy<IReadOnlyDataColumn> CreateColumn(
            Type columnType,
            SerializedColumn serializedColumn)
        {
            if (DataColumnFactories.TryGetValue(columnType, out var columnFactory))
            {
                return new Lazy<IReadOnlyDataColumn>(() =>
                {
                    var column = columnFactory(serializedColumn.ItemCount);

                    column.Deserialize(serializedColumn);

                    return column;
                });
            }
            else
            {
                throw new ArgumentException(
                    $"Column type '{columnType.Name}' isn't supported",
                    nameof(columnType));
            }
        }
        #endregion

        protected override int RecordCount => _recordCount;

        protected override IReadOnlyDataColumn GetDataColumn(int columnIndex)
        {
            if (columnIndex < 0 || columnIndex >= _dataColumns.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(columnIndex), columnIndex.ToString());
            }

            return _dataColumns[columnIndex].Value;
        }
    }
}