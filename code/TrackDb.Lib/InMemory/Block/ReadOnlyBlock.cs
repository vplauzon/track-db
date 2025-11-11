using System;
using System.Buffers.Binary;
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
            var columnCount = schema.Columns.Count;
            var columnPayloads = new ReadOnlyMemory<byte>[columnCount];
            var dataColumns = new Lazy<IReadOnlyDataColumn>[columnCount];
            var columnPayloadPosition = reader.Position;

            for (var i = 0; i != columnCount; ++i)
            {
                var payloadSize = reader.ReadUInt16();

                columnPayloads[i] = payload.Slice(columnPayloadPosition, payloadSize);
                columnPayloadPosition += payloadSize;
            }

            var hasNulls = BitPacker.Unpack(
                payload.Span.Slice(columnPayloadPosition, BitPacker.PackSize(itemCount, 1)),
                itemCount,
                1)
                .ToImmutableArray(i => Convert.ToBoolean(i));

            for (var i = 0; i != dataColumns.Length; ++i)
            {
                var columnPayload = columnPayloads[i];

                dataColumns[i] = CreateColumn(
                    schema.Columns[i].ColumnType,
                    itemCount,
                    hasNulls[i],
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
            bool hasNulls,
            ReadOnlyMemory<byte> payload)
        {
            return new Lazy<IReadOnlyDataColumn>(() =>
            {
                var column = CreateDataColumn(columnType, itemCount);

                column.Deserialize(itemCount, hasNulls, payload);

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