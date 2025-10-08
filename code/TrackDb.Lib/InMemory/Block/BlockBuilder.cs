﻿using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block.SpecializedColumn;

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

        private const int START_TRUNCATE_LENGTH = 100;
        private const int MAX_TRUNCATE_NAIVE = 500;
        private const int MAX_TRUNCATE_ROW_COUNT = short.MaxValue;

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
            return Serialize(null);
        }

        private int GetSerializeSize(int rowCount)
        {
            return Serialize(rowCount).Payload.Length;
        }

        private SerializedBlock Serialize(int? rowCount)
        {
            if (rowCount > ((IBlock)this).RecordCount)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(rowCount),
                    $"{rowCount} > {((IBlock)this).RecordCount}");
            }

            var serializedColumns = _dataColumns
                .Select(c => c.Serialize(rowCount))
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
            {
                var truncateRowCount = GetTruncateRowCount(maxSize);
                var columnCount = block.TableSchema.Columns.Count;
                var newBlock = new BlockBuilder(block.TableSchema);
                //  Include record ID
                var records = block.Project(
                    new object?[columnCount + 1].AsMemory(),
                    Enumerable.Range(0, columnCount + 1),
                    Enumerable.Range(0, truncateRowCount),
                    0);

                foreach (var record in records)
                {
                    newBlock.AppendRecord(
                        (long)record.Span[columnCount]!,
                        record.Span.Slice(0, columnCount));
                }

                return newBlock;
            }
        }

        private int GetTruncateRowCount(int maxSize)
        {
            IBlock block = this;
            var maxRowCount = Math.Min(MAX_TRUNCATE_ROW_COUNT, block.RecordCount);
            var startingRowCount = Math.Min(
                maxRowCount,
                maxRowCount <= MAX_TRUNCATE_NAIVE ? MAX_TRUNCATE_NAIVE : START_TRUNCATE_LENGTH);
            var startingSize = GetSerializeSize(startingRowCount);
            (var lowerBound, var upperBound) = GrowTruncationBounds(
                maxSize,
                maxRowCount,
                new TruncationBound(0, 0),
                new TruncationBound(startingRowCount, startingSize));
            (var optimalRowCount, var iterationCount) =
                OptimizeTruncationRowCount(maxSize, maxRowCount, lowerBound, upperBound, 1);

            return optimalRowCount;
        }

        private (TruncationBound lowerBound, TruncationBound upperBound) GrowTruncationBounds(
            int maxSize,
            int maxRowCount,
            TruncationBound lowerBound,
            TruncationBound upperBound)
        {
            if (upperBound.Size >= maxSize || upperBound.RecordCount == maxRowCount)
            {
                return (lowerBound, upperBound);
            }
            else
            {
                var newCount = Math.Min(maxRowCount, upperBound.RecordCount * 2);

                return GrowTruncationBounds(
                    maxSize,
                    maxRowCount,
                    upperBound,
                    new TruncationBound(newCount, GetSerializeSize(newCount)));
            }
        }

        private (int recordCount, int iterationCount) OptimizeTruncationRowCount(
            int maxSize,
            int maxRowCount,
            TruncationBound lowerBound,
            TruncationBound upperBound,
            int iterationCount)
        {
            const int DELTA_SIZE_TOLERANCE = 10;

            if (upperBound.Size <= maxSize)
            {
                return (upperBound.RecordCount, iterationCount);
            }
            else if (lowerBound.RecordCount == upperBound.RecordCount - 1)
            {
                return (lowerBound.RecordCount, iterationCount);
            }
            else
            {
                //var newCount = (lowerBound.RecordCount + upperBound.RecordCount) / 2;
                //  Interpolate the new count
                //  Assuming X = count & Y = size
                //  Also assuming that Y = m.X + b (it's linear)
                //  We want Y to converge to maxSize or Y3 = maxSize
                //  Y1 = m.X1 + b, Y2 = m.X2 + b
                //  => Y2-Y1 = m.(X2-X1) => m = (Y2-Y1)/(X2-X1)
                //  => b = Y1-m.X1
                //  Then, Y3 = m.X3 + b => X3 = (Y3-b)/m
                var m = ((double)(upperBound.Size - lowerBound.Size))
                    / (upperBound.RecordCount - lowerBound.RecordCount);
                var b = lowerBound.Size - m * lowerBound.RecordCount;
                var newCount = (int)((maxSize - b) / m);
                var newSize = GetSerializeSize(newCount);
                var newBound = new TruncationBound(newCount, newSize);
                var newLowerBound = newSize > maxSize ? lowerBound : newBound;
                var newUpperBound = newSize > maxSize ? newBound : upperBound;

                if (Math.Abs(newSize - lowerBound.Size) < DELTA_SIZE_TOLERANCE
                    || Math.Abs(newSize - upperBound.Size) < DELTA_SIZE_TOLERANCE)
                {
                    return (lowerBound.RecordCount, iterationCount);
                }
                else
                {
                    return OptimizeTruncationRowCount(
                        maxSize,
                        maxRowCount,
                        newLowerBound,
                        newUpperBound,
                        iterationCount + 1);
                }
            }
        }
        #endregion
    }
}