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
        #region Inner types
        private record TruncationBound(int RecordCount, int Size);
        #endregion

        private const int MAX_TRUNCATE_OPTIMIZATION_ROUNDS = 5;
        private static readonly IImmutableDictionary<Type, Func<int, IDataColumn>> _dataColumnFactories =
            CreateDataColumnFactories();

        #region Constructors
        public BlockBuilder(TableSchema schema)
            : base(
                  schema,
                  schema.Columns
                  .Select(c => CreateColumn(c.ColumnType, 0))
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

        private static IDataColumn CreateColumn(Type columnType, int capacity)
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
        public void AppendBlock(IBlock block)
        {
            var data = block.Query(
                AllInPredicate.Instance,
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
                        RecordIndex = recordIndex
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

        #region Truncation helpers
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

        /// <summary>
        /// Extract a number of rows from the block builder.  The resulting
        /// block should serialize to as close to but <= <paramref name="maxSize"/>.
        /// </summary>
        /// <param name="maxSize"></param>
        /// <returns></returns>
        public BlockBuilder PrefixTruncateBlock(int maxSize)
        {
            IBlock block = this;
            var totalRowCount = block.RecordCount;

            if (totalRowCount == 0)
            {
                return new BlockBuilder(block.TableSchema);
            }
            else
            {   //  We start at 100 to limit the resource utilization in serialization
                var startingRowCount = Math.Min(100, totalRowCount);
                var newBlock = CreateTruncatedBlock(startingRowCount);
                var size = newBlock.Serialize().Payload.Length;

                if (size < maxSize && startingRowCount == totalRowCount)
                {
                    return newBlock;
                }
                else
                {   //  Allow GC
                    newBlock = null;

                    return OptimizePrefixTruncation(
                        new TruncationBound(0, 0),
                        new TruncationBound(startingRowCount, size),
                        maxSize,
                        1);
                }
            }
        }

        private BlockBuilder CreateTruncatedBlock(int rowCount)
        {
            var block = (IBlock)this;
            var columnCount = block.TableSchema.Columns.Count;
            var newBlock = new BlockBuilder(block.TableSchema);
            var records = block.Query(
                AllInPredicate.Instance,
                Enumerable.Range(0, columnCount + 1))
                .Take(rowCount);

            foreach (var record in records)
            {
                newBlock.AppendRecord(
                    (long)record.Span[columnCount]!,
                    record.Span.Slice(0, columnCount));
            }

            return newBlock;
        }

        private BlockBuilder OptimizePrefixTruncation(
            TruncationBound lowerTruncationBound,
            TruncationBound upperTruncationBound,
            int maxSize,
            int round)
        {
            IBlock block = this;
            //  Assume lower & upper bound are on a line
            //  y = m.x + b (y = size, x = record count)
            //  => y2-y1 = m.(x2-x1) => m = (y2-y1)/(x2-x1) => b = y2-m.x2
            //  x3 = (y3-b)/m (interpolated record count)
            var slope = (upperTruncationBound.Size - lowerTruncationBound.Size)
                / (upperTruncationBound.RecordCount - lowerTruncationBound.RecordCount);
            var bias = upperTruncationBound.Size - slope * upperTruncationBound.RecordCount;
            var interpolatedCount = Math.Min(
                short.MaxValue,
                Math.Min(block.RecordCount, (maxSize - bias) / slope));
            var newBlock = CreateTruncatedBlock(interpolatedCount);
            var size = newBlock.Serialize().Payload.Length;

            if (interpolatedCount > upperTruncationBound.RecordCount
                && size <= upperTruncationBound.Size)
            {
                throw new InvalidOperationException("Interpolation failed");
            }
            if (interpolatedCount < upperTruncationBound.RecordCount
                && size >= upperTruncationBound.Size)
            {
                throw new InvalidOperationException("Interpolation failed");
            }
            if (size <= maxSize
                && (Math.Abs(size - maxSize) / ((double)maxSize) < 0.05
                || round >= MAX_TRUNCATE_OPTIMIZATION_ROUNDS))
            {
                return newBlock;
            }
            else
            {
                if (upperTruncationBound.Size <= size)
                {   //  We have y1, y3, y2
                    return OptimizePrefixTruncation(
                        lowerTruncationBound,
                        new TruncationBound(interpolatedCount, size),
                        maxSize,
                        round + 1);
                }
                else
                {   //  We have y1, y2, y3
                    return OptimizePrefixTruncation(
                        upperTruncationBound,
                        new TruncationBound(interpolatedCount, size),
                        maxSize,
                        round + 1);
                }
            }
        }
        #endregion
    }
}