using Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn;
using Ipdb.Lib2.Query;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class BlockBuilder : ReadOnlyBlockBase
    {
        private static readonly IImmutableDictionary<Type, Func<int, IDataColumn>> _dataColumnFactories =
            CreateDataColumnFactories();

        #region Constructors
        public BlockBuilder(TableSchema schema)
            : base(
                  schema,
                  schema.Columns
                  .Select(c => CreateCachedColumn(c.ColumnType, 0))
                  //  Record ID column
                  .Append(new ArrayLongColumn(false, 0)))
        {
            DataColumns = base.DataColumns.Cast<IDataColumn>().ToImmutableArray();
        }

        private static IImmutableDictionary<Type, Func<int, IDataColumn>> CreateDataColumnFactories()
        {
            var builder = ImmutableDictionary<Type, Func<int, IDataColumn>>.Empty.ToBuilder();

            builder.Add(typeof(int), capacity => new ArrayIntColumn(false, capacity));
            builder.Add(typeof(int?), capacity => new ArrayIntColumn(true, capacity));
            builder.Add(typeof(long), capacity => new ArrayLongColumn(false, capacity));
            builder.Add(typeof(long?), capacity => new ArrayLongColumn(true, capacity));
            builder.Add(typeof(string), capacity => new ArrayStringColumn(true, capacity));

            return builder.ToImmutableDictionary();
        }

        private static IDataColumn CreateCachedColumn(Type columnType, int capacity)
        {
            if (_dataColumnFactories.TryGetValue(columnType, out var factory))
            {
                return factory(capacity);
            }
            else
            {
                throw new NotSupportedException($"Column type:  '{columnType}'");
            }
        }
        #endregion

        public static IImmutableSet<Type> SupportedDataColumnTypes { get; } =
            _dataColumnFactories.Keys
            .ToImmutableHashSet();

        protected new IImmutableList<IDataColumn> DataColumns { get; }

        #region Writable block methods
        public SerializedBlock Serialize()
        {
            var serializedColumns = DataColumns
                .Select(c => c.Serialize())
                .ToImmutableArray();
            var columnMinima = serializedColumns
                .Select(c => c.ColumnMinimum)
                .ToImmutableArray();
            var columnMaxima = serializedColumns
                .Select(c => c.ColumnMaximum)
                .ToImmutableArray();
            var payloadSizes = serializedColumns
                .Select(c => (short)c.Payload.Length)
                .ToImmutableArray();
            var payloadSizesSize = sizeof(short) * payloadSizes.Length;
            var blockPayload = new byte[
                payloadSizesSize
                + payloadSizes.Select(i => (int)i).Sum()];
            var sizeSpan = blockPayload.AsSpan().Slice(0, payloadSizesSize);

            for (int i = 0; i != payloadSizes.Length; ++i)
            {
                //  Write column payload size to the block header
                BinaryPrimitives.WriteUInt16LittleEndian(
                    sizeSpan.Slice(sizeof(short) * i, sizeof(short)),
                    (UInt16)payloadSizes[i]);
                //  Write column payload within block payload
                serializedColumns[i].Payload.CopyTo(
                    blockPayload.AsMemory().Slice(
                        payloadSizesSize + serializedColumns.Take(i).Select(c => c.Payload.Length).Sum(),
                        serializedColumns[i].Payload.Length));
            }

            return new SerializedBlock(
                serializedColumns.First().ItemCount,
                columnMinima,
                columnMaxima,
                blockPayload);
        }

        public void AppendBlock(IBlock block)
        {
            var data = block.Query(
                new AllInPredicate(),
                //  Include record ID
                Enumerable.Range(0, Schema.Columns.Count + 1));

            if (!block.TableSchema.AreColumnsCompatible(Schema.Columns))
            {
                throw new ArgumentException("Columns are incompatible", nameof(block));
            }

            //  Copy data
            foreach (var row in data)
            {
                for (var columnIndex = 0; columnIndex != Schema.Columns.Count + 1; ++columnIndex)
                {
                    DataColumns[columnIndex].AppendValue(row.Span[columnIndex]);
                }
            }
        }

        public void AppendRecord(long recordId, ReadOnlySpan<object?> record)
        {
            if (record.Length != DataColumns.Count - 1)
            {
                throw new ArgumentException(
                    $"Expected {DataColumns.Count - 1} columns but is {record.Length}",
                    nameof(record));
            }
            for (int i = 0; i != record.Length; ++i)
            {
                DataColumns[i].AppendValue(record[i]);
            }
            DataColumns[record.Length].AppendValue(recordId);
        }

        /// <summary>Tries to delete records passed in.</summary>
        /// <param name="recordIds"></param>
        /// <returns>The deleted record IDs.</returns>
        public IEnumerable<long> DeleteRecords(IEnumerable<long> recordIds)
        {
            var recordIdSet = recordIds.ToImmutableHashSet();

            if (recordIdSet.Any() && DataColumns.First().RecordCount > 0)
            {
                var columns = new object?[Schema.Columns.Count];
                var recordIdColumn = (ArrayLongColumn)DataColumns.Last();
                var deletedRecordPairs = Enumerable.Range(0, DataColumns.First().RecordCount)
                    .Select(recordIndex => new
                    {
                        RecordId = recordIdColumn.RawData[recordIndex],
                        RecordIndex = (short)recordIndex
                    })
                    .Where(o => recordIdSet.Contains(o.RecordId))
                    .OrderBy(o => o.RecordIndex)
                    .ToImmutableArray();

                if (deletedRecordPairs.Any())
                {
                    var deletedRecordIndexes = deletedRecordPairs
                        .Select(o => o.RecordIndex)
                        .ToImmutableArray();

                    foreach (var dataColumn in DataColumns)
                    {
                        dataColumn.DeleteRecords(deletedRecordIndexes);
                    }

                    return deletedRecordPairs
                        .Select(o => o.RecordId);
                }
            }

            return Array.Empty<long>();
        }
        #endregion
    }
}