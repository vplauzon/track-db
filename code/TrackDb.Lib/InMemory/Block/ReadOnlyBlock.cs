using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Encoding;

namespace TrackDb.Lib.InMemory.Block
{
    internal class ReadOnlyBlock : ReadOnlyBlockBase
    {
        private readonly IImmutableList<Lazy<IReadOnlyDataColumn>> _dataColumns;
        private readonly int _recordCount;

        #region Constructors
        /// <summary>Load a block from a serialized stream.</summary>
        /// <remarks>
        /// Does the reverse from
        /// <see cref="BlockBuilder.Serialize(int?, Memory{byte}, ByteWriter)"/>.
        /// </remarks>
        /// <param name="payload"></param>
        /// <param name="schema"></param>
        /// <returns></returns>
        public static IBlock Load(ReadOnlyMemory<byte> payload, TableSchema schema)
        {
            var reader = new ByteReader(payload.Span);
            var itemCount = reader.ReadUInt16();
            var columnCount = schema.ColumnProperties.Count;
            var columnSizeReader = reader.SliceArrayUInt16(columnCount);
            var dataColumns = new Lazy<IReadOnlyDataColumn>[columnCount];

            for (var i = 0; i != columnCount; ++i)
            {
                var payloadSize = columnSizeReader.ReadUInt16();
                var columnPayload = reader.SliceForward(payloadSize);

                dataColumns[i] = CreateColumn(
                    schema.ColumnProperties[i].ColumnSchema.ColumnType,
                    itemCount,
                    columnPayload);
            }

            return new ReadOnlyBlock(schema, itemCount, dataColumns);
        }

        private ReadOnlyBlock(
            TableSchema schema,
            int recordCount,
            IEnumerable<Lazy<IReadOnlyDataColumn>> dataColumns)
            : base(schema)
        {
            _dataColumns = dataColumns.ToImmutableArray();
            _recordCount = recordCount;
        }

        private static Lazy<IReadOnlyDataColumn> CreateColumn(
            Type columnType,
            int itemCount,
            ReadOnlySpan<byte> payload)
        {   //  We copy the payload so it can be individually GCed
            var copy = payload.ToArray();

            return new Lazy<IReadOnlyDataColumn>(() =>
            {
                var column = CreateDataColumn(columnType, itemCount);

                column.Deserialize(itemCount, copy);

                return column;
            });
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