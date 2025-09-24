using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.InMemory.Block
{
    internal class BlockBuilder : ReadOnlyBlockBase
    {
        #region Inner types
        private record TruncationBound(
            //  Number of record in the block
            int RecordCount,
            //  Size (in bytes) of the block
            int Size)
        {
            public override string ToString()
            {
                return $"(Count={RecordCount}, Size={Size})";
            }
        }
        #endregion

        private const int MAX_TRUNCATE_OPTIMIZATION_ROUNDS = 5;

        private readonly IImmutableList<IDataColumn> _dataColumns;

        protected override int RecordCount => _dataColumns.First().RecordCount;

        protected override IReadOnlyDataColumn GetDataColumn(int columnIndex)
        {
            if (columnIndex < 0 || columnIndex >= _dataColumns.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(columnIndex), columnIndex.ToString());
            }

            return _dataColumns[columnIndex];
        }

        #region Constructors
        public BlockBuilder(TableSchema schema)
            : base(schema)
        {
            _dataColumns = schema.Columns
                .Select(c => CreateDataColumn(c.ColumnType, 0))
                //  Record ID column
                .Append(new ArrayLongColumn(false, 0))
                .ToImmutableArray();
        }
        #endregion

        #region Writable block methods
        public void AppendBlock(IBlock block)
        {
            if (!block.TableSchema.AreColumnsCompatible(Schema.Columns))
            {
                throw new ArgumentException("Columns are incompatible", nameof(block));
            }

            //  Include record ID
            var data = block.Project(
                new object?[Schema.Columns.Count + 1].AsMemory(),
                Enumerable.Range(0, Schema.Columns.Count + 1),
                Enumerable.Range(0, block.RecordCount),
                0);

            //  Copy data
            foreach (var row in data)
            {
                for (var columnIndex = 0; columnIndex != Schema.Columns.Count + 1; ++columnIndex)
                {
                    _dataColumns[columnIndex].AppendValue(row.Span[columnIndex]);
                }
            }
        }

        public void OrderByRecordId()
        {
            IBlock block = this;

            if (block.RecordCount > 1)
            {
                var recordIdColumn = _dataColumns.Last();
                var recordIds = Enumerable.Range(0, block.RecordCount)
                    .Select(i => ((long?)recordIdColumn.GetValue(i))!.Value)
                    .ToImmutableArray();
                var isSorted = recordIds
                    .Zip(recordIds.Skip(1), (a, b) => a <= b)
                    .All(x => x);

                if (!isSorted)
                {
                    var orderIndexes = recordIds
                        .Zip(Enumerable.Range(0, recordIds.Length))
                        .OrderBy(p => p.First)
                        .Select(p => p.Second)
                        .ToImmutableArray();

                    foreach (var column in _dataColumns)
                    {
                        column.Reorder(orderIndexes);
                    }
                }
            }
        }

        public void AppendRecord(long recordId, ReadOnlySpan<object?> record)
        {
            if (record.Length != _dataColumns.Count - 1)
            {
                throw new ArgumentException(
                    $"Expected {_dataColumns.Count - 1} columns but is {record.Length}",
                    nameof(record));
            }
            for (int i = 0; i != record.Length; ++i)
            {
                _dataColumns[i].AppendValue(record[i]);
            }
            _dataColumns[record.Length].AppendValue(recordId);
        }

        /// <summary>Tries to delete records passed in.</summary>
        /// <param name="recordIds"></param>
        /// <returns>The deleted record IDs.</returns>
        public IEnumerable<long> DeleteRecordsByRecordId(IEnumerable<long> recordIds)
        {
            var recordIdSet = recordIds.ToImmutableHashSet();

            if (recordIdSet.Any() && _dataColumns.First().RecordCount > 0)
            {
                var columns = new object?[Schema.Columns.Count];
                var recordIdColumn = (ArrayLongColumn)_dataColumns.Last();
                var deletedRecordPairs = Enumerable.Range(0, _dataColumns.First().RecordCount)
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

                    DeleteRecordsByRecordIndex(deletedRecordIndexes);

                    return deletedRecordPairs
                        .Select(o => o.RecordId);
                }
            }

            return Array.Empty<long>();
        }

        /// <summary>Delete record indexes.</summary>
        /// <param name="recordIndexes"></param>
        public void DeleteRecordsByRecordIndex(IEnumerable<int> recordIndexes)
        {
            foreach (var dataColumn in _dataColumns)
            {
                dataColumn.DeleteRecords(recordIndexes);
            }
        }
        #endregion

        #region Serialization
        /// <summary>
        /// A block is serialized with the following items:
        /// For each column we persist a UInt16 for the column payload size.
        /// Then for each column, we persist its payload.
        /// Everything else is metadata outside the block, captured in
        /// <see cref="SerializedBlock"/>.
        /// </summary>
        /// <returns></returns>
        public SerializedBlock Serialize()
        {
            var serializedColumns = _dataColumns
                .Select(c => c.Serialize())
                .ToImmutableArray();

            return new SerializedBlock(serializedColumns);
        }

        /// <summary>
        /// Extract a number of rows from the block builder.  The resulting
        /// block should serialize to as close to but <= <paramref name="maxSize"/>.
        /// </summary>
        /// <param name="maxSize"></param>
        /// <returns></returns>
        public BlockBuilder TruncateBlock(int maxSize)
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

                if (size > maxSize || startingRowCount != totalRowCount)
                {
                    newBlock = null;    //  Allow GC
                    newBlock = OptimizePrefixTruncation(
                        new TruncationBound(0, 0),
                        new TruncationBound(startingRowCount, size),
                        maxSize,
                        1);
                    DeleteRecordsByRecordIndex(
                        Enumerable.Range(0, ((IBlock)newBlock).RecordCount));
                }

                return newBlock;
            }
        }

        private BlockBuilder CreateTruncatedBlock(int rowCount)
        {
            var block = (IBlock)this;
            var columnCount = block.TableSchema.Columns.Count;
            var newBlock = new BlockBuilder(block.TableSchema);
            //  Include record ID
            var records = block.Project(
                new object?[columnCount + 1].AsMemory(),
                Enumerable.Range(0, columnCount + 1),
                Enumerable.Range(0, Math.Min(block.RecordCount, rowCount)),
                0);

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
            int iteration)
        {
            IBlock block = this;
            //  Assume lower & upper bound are on a line
            //  y = m.x + b (y = size, x = record count)
            //  1:  lower bound, 2:  upper bound, 3:  interpolation
            //  => y2-y1 = m.(x2-x1) => m = (y2-y1)/(x2-x1) => b = y2-m.x2
            //  y3 = maxSize (we try to hit that sweet spot)
            //  x3 = (y3-b)/m (interpolated record count)
            //  Note:  x3 > x2 is possible (because we do not try the maximum value at first)
            var slope = (double)(upperTruncationBound.Size - lowerTruncationBound.Size)
                / (upperTruncationBound.RecordCount - lowerTruncationBound.RecordCount);
            var bias = upperTruncationBound.Size - slope * upperTruncationBound.RecordCount;
            var interpolatedCount = (int)Math.Ceiling((maxSize - bias) / slope);
            var newBlock = CreateTruncatedBlock(interpolatedCount);

            if (interpolatedCount == lowerTruncationBound.RecordCount
                || interpolatedCount == upperTruncationBound.RecordCount)
            {   //  Interpolation didn't move boundaries
                if (interpolatedCount == 0)
                {
                    throw new InvalidOperationException("Interpolation failed:  can't move from zero");
                }
                else
                {
                    return newBlock;
                }
            }
            else if (interpolatedCount >= block.RecordCount)
            {
                return newBlock;
            }
            else
            {
                var size = newBlock.Serialize().Payload.Length;
                var interpolatedBound = new TruncationBound(interpolatedCount, size);

                if (size == maxSize)
                {
                    return newBlock;
                }
                else if (interpolatedBound.RecordCount < upperTruncationBound.RecordCount)
                {   //  Case of x3<x2
                    if (size > maxSize)
                    {
                        return OptimizePrefixTruncation(
                            lowerTruncationBound,
                            interpolatedBound,
                            maxSize,
                            iteration + 1);
                    }
                    else
                    {
                        return OptimizePrefixTruncation(
                            interpolatedBound,
                            upperTruncationBound,
                            maxSize,
                            iteration + 1);
                    }
                }
                else
                {   //  Case of x3>x2
                    if (upperTruncationBound.Size > maxSize)
                    {
                        throw new InvalidOperationException(
                            $"Failed interpolation:  interpolated beyond maximum ; " +
                            $"{lowerTruncationBound}, {upperTruncationBound}, " +
                            $"{interpolatedBound}");
                    }

                    return OptimizePrefixTruncation(
                        upperTruncationBound,
                        interpolatedBound,
                        maxSize,
                        iteration + 1);
                }
            }
        }
        #endregion
    }
}