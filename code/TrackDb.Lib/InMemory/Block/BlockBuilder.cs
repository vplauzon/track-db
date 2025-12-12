using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.Encoding;
using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using TrackDb.Lib.Logging;

namespace TrackDb.Lib.InMemory.Block
{
    internal class BlockBuilder : ReadOnlyBlockBase
    {
        private const int START_TRUNCATE_ROW_COUNT = 100;
        private const int MAX_TRUNCATE_ROW_COUNT = short.MaxValue;
        private const int MAX_ITERATION_COUNT = 5;

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
                //  CreationTime column
                .Append(new ArrayDateTimeColumn(false, 0))
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
                new object?[_dataColumns.Count].AsMemory(),
                Enumerable.Range(0, _dataColumns.Count).ToImmutableArray(),
                Enumerable.Range(0, block.RecordCount),
                0);

            //  Copy data
            foreach (var row in data)
            {
                for (var columnIndex = 0; columnIndex != _dataColumns.Count; ++columnIndex)
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

        public void AppendRecord(DateTime creationTime, long recordId, ReadOnlySpan<object?> record)
        {
            if (record.Length != _dataColumns.Count - 2)
            {
                throw new ArgumentException(
                    $"Expected {_dataColumns.Count - 2} columns but is {record.Length}",
                    nameof(record));
            }
            for (int i = 0; i != record.Length; ++i)
            {
                _dataColumns[i].AppendValue(record[i]);
            }
            _dataColumns[Schema.CreationTimeColumnIndex]
                .AppendValue(creationTime);
            _dataColumns[Schema.RecordIdColumnIndex]
                .AppendValue(recordId);
        }

        /// <summary>Deletes all records in the block.</summary>
        public void DeleteAll()
        {
            DeleteRecordsByRecordIndex(Enumerable.Range(0, ((IBlock)this).RecordCount));
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
                var recordIdColumn = (ArrayLongColumn)_dataColumns[Schema.RecordIdColumnIndex];
                var deletedRecordPairs = Enumerable.Range(0, _dataColumns.First().RecordCount)
                    .Select(recordIndex => new
                    {
                        RecordId = recordIdColumn.RawData[recordIndex],
                        RecordIndex = recordIndex
                    })
                    .Where(o => recordIdSet.Contains(o.RecordId))
                    .ToImmutableArray();

                if (deletedRecordPairs.Any())
                {
                    var deletedRecordIndexes = deletedRecordPairs
                        .Select(o => o.RecordIndex);

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
        public BlockStats Serialize(Memory<byte> buffer)
        {
            return SerializeSegment(buffer, 0, ((IBlock)this).RecordCount);
        }

        private BlockStats SerializeSegment(Memory<byte>? buffer, int skipRows, int takeRows)
        {
            IBlock block = this;
            var writer =
                new ByteWriter(buffer != null ? buffer.Value.Span : new Span<byte>(), false);

            if (skipRows < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(skipRows));
            }
            if (takeRows < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(takeRows));
            }
            if (skipRows > block.RecordCount)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(skipRows),
                    $"{skipRows} > {block.RecordCount}");
            }
            if (skipRows + takeRows > block.RecordCount)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(takeRows),
                    $"{skipRows} + {takeRows} > {block.RecordCount}");
            }
            //  Item count
            writer.WriteUInt16((ushort)takeRows);

            //  Column payload sizes
            var columnsPayloadSizePlaceholder = writer.PlaceholderArrayUInt16(_dataColumns.Count);
            var columnStatsBuilder = ImmutableArray<ColumnStats>.Empty.ToBuilder();

            //  Body:  columns
            for (var i = 0; i != _dataColumns.Count; ++i)
            {
                var sizeBefore = writer.Position;
                var columnStats = _dataColumns[i].SerializeSegment(ref writer, skipRows, takeRows);
                var columnSize = (ushort)(writer.Position - sizeBefore);

                columnStatsBuilder.Add(columnStats);
                columnsPayloadSizePlaceholder.SetValue(i, columnSize);
            }

            //  Footer:  has nulls
            BitPacker.Pack(
                columnStatsBuilder.Select(c => Convert.ToUInt64(c.HasNulls)),
                columnStatsBuilder.Count,
                1,
                ref writer);

            return new(takeRows, writer.Position, columnStatsBuilder.ToImmutable());
        }

        /// <summary>
        /// Extract a number of rows from the block builder.  The resulting
        /// block should serialize to as close to but <= <paramref name="maxSize"/>.
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="skipRows">Number of rows to skip at the beginning of the block.</param>
        /// <param name="hintRowCount">
        /// Hint at what row count should be.  This eliminates the first iterations of guessing row
        /// count.
        /// </param>
        /// <returns></returns>
        public BlockStats TruncateSerialize(Memory<byte> buffer, int skipRows, int? hintRowCount)
        {
            IBlock block = this;
            var maxSize = buffer.Length;
            var totalRowCount = block.RecordCount - skipRows;

            if (skipRows < 0 || skipRows >= block.RecordCount)
            {
                throw new ArgumentOutOfRangeException(nameof(skipRows));
            }

            var maxRowCount = Math.Min(MAX_TRUNCATE_ROW_COUNT, totalRowCount);
            var startingRowCount = Math.Min(maxRowCount, hintRowCount ?? START_TRUNCATE_ROW_COUNT);
            var startingUpperBound = SerializeSegment(buffer, skipRows, startingRowCount);
            var finalStats = OptimizeTruncationRowCount(
                buffer,
                skipRows,
                maxRowCount,
                BlockStats.Empty,
                startingUpperBound,
                1);

            if (finalStats.ItemCount <= 0)
            {
                throw new InvalidOperationException(
                    $"Can't serialize zero-rows block {finalStats.ItemCount} for " +
                    $"table '{Schema.TableName}'");
            }
            if (finalStats.Size > maxSize)
            {
                throw new OverflowException(
                    $"Block buffer overflow:  {finalStats.Size} > {maxSize}");
            }
            //  Reserialize so the buffer is up-to-date

            return SerializeSegment(buffer, skipRows, finalStats.ItemCount);
        }

        private BlockStats OptimizeTruncationRowCount(
            Memory<byte> buffer,
            int skipRows,
            int maxRowCount,
            BlockStats lowerBound,
            BlockStats upperBound,
            int iterationCount)
        {
            const int DELTA_SIZE_TOLERANCE = 25;

            var maxSize = buffer.Length;

            if (upperBound.Size <= maxSize && upperBound.Size > maxSize - DELTA_SIZE_TOLERANCE)
            {
                return upperBound;
            }
            else if (lowerBound.Size > maxSize - DELTA_SIZE_TOLERANCE)
            {
                return lowerBound;
            }
            else if (upperBound.ItemCount - lowerBound.ItemCount <= 1)
            {
                return upperBound.Size <= maxSize ? upperBound : lowerBound;
            }
            else
            {
                //  Inter / extra-polate the new count
                //  Assuming X = count & Y = size
                //  Also assuming that Y = m.X + b (relation is linear)
                //  We want Y to converge to maxSize or Y3 = maxSize
                //  Y1 = m.X1 + b, Y2 = m.X2 + b
                //  => Y2-Y1 = m.(X2-X1) => m = (Y2-Y1)/(X2-X1)
                //  => b = Y1-m.X1
                //  Then, Y3 = m.X3 + b => X3 = (Y3-b)/m
                var m = (double)(upperBound.Size - lowerBound.Size)
                    / (upperBound.ItemCount - lowerBound.ItemCount);
                var b = lowerBound.Size - m * lowerBound.ItemCount;
                var newCount = Math.Min(maxRowCount, (int)((maxSize - b) / m));
                var newBound = SerializeSegment(buffer, skipRows, newCount);
                //  Now we have X1, X2 & X3 with X1<X2 but X3 could be anywhere
                //  We'll first reorder them into XA, XB & XC with XA<XB<XC
                (var boundA, var boundB, var boundC) =
                    SortByItemCount(lowerBound, upperBound, newBound);
                //  In general, we want to move up in size, but we want to keep lower bound
                //  under max-size
                (var newLowerBound, var newUpperBound) = boundB.Size > maxSize && boundC.Size > maxSize
                    ? (boundA, boundB)
                    : (boundB, boundC);

                if ((Math.Abs(newLowerBound.Size - lowerBound.Size) < DELTA_SIZE_TOLERANCE
                    && Math.Abs(newUpperBound.Size - upperBound.Size) < DELTA_SIZE_TOLERANCE
                    && newLowerBound.ItemCount > 0)
                    || iterationCount == MAX_ITERATION_COUNT)
                {
                    return newUpperBound.Size <= maxSize
                        ? newUpperBound
                        : newLowerBound;
                }
                else
                {
                    return OptimizeTruncationRowCount(
                        buffer,
                        skipRows,
                        maxRowCount,
                        newLowerBound,
                        newUpperBound,
                        iterationCount + 1);
                }
            }
        }

        private (BlockStats boundA, BlockStats boundB, BlockStats boundC) SortByItemCount(
            BlockStats bound1,
            BlockStats bound2,
            BlockStats bound3)
        {
            if (bound2.ItemCount < bound1.ItemCount)
            {
                (bound1, bound2) = (bound2, bound1);
            }
            if (bound3.ItemCount < bound2.ItemCount)
            {
                (bound2, bound3) = (bound3, bound2);
            }
            if (bound2.ItemCount < bound1.ItemCount)
            {
                (bound1, bound2) = (bound2, bound1);
            }

            return (bound1, bound2, bound3);
        }
        #endregion

        #region Log
        public NewRecordsContent ToLog()
        {
            var recordCount = RecordCount;
            var creationTimes = Enumerable.Range(0, recordCount)
                .Select(i => (DateTime)_dataColumns[Schema.CreationTimeColumnIndex].GetValue(i)!)
                .ToImmutableList();
            var newRecordIds = Enumerable.Range(0, recordCount)
                .Select(i => (long)_dataColumns[Schema.RecordIdColumnIndex].GetValue(i)!)
                .ToImmutableList();
            var columns = Enumerable.Range(0, Schema.Columns.Count)
                .Select(i => KeyValuePair.Create(
                    Schema.Columns[i].ColumnName,
                    _dataColumns[i].GetLogValues().ToList()))
                .ToImmutableDictionary();

            return new(creationTimes, newRecordIds, columns);
        }

        public void AppendLog(NewRecordsContent content)
        {
            for (var i = 0; i != Schema.Columns.Count; ++i)
            {
                _dataColumns[i].AppendLogValues(content.Columns[Schema.Columns[i].ColumnName]);
            }
            //  Creation time
            foreach (var creationTime in content.CreationTimes)
            {
                _dataColumns[Schema.CreationTimeColumnIndex].AppendValue(creationTime);
            }
            //  Record ID
            foreach (var newRecordId in content.NewRecordIds)
            {
                _dataColumns[Schema.RecordIdColumnIndex].AppendValue(newRecordId);
            }
        }
        #endregion
    }
}