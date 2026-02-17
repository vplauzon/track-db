using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text.Json;
using TrackDb.Lib.Encoding;
using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using TrackDb.Lib.Logging;

namespace TrackDb.Lib.InMemory.Block
{
    internal class BlockBuilder : ReadOnlyBlockBase
    {
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
        public BlockBuilder(TableSchema schema, int capacity = 0)
            : base(schema)
        {
            _dataColumns = schema.ColumnProperties
                .Select(c => CreateDataColumn(c.ColumnSchema.ColumnType, capacity))
                .ToImmutableArray();
        }

        public static BlockBuilder MergeBlocks(params IEnumerable<IBlock> blocks)
        {
            var materializedBlocks = blocks.ToArray();

            if (materializedBlocks.Length == 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(blocks),
                    "Should be at least one block");
            }

            //  Pre-provision the full capacity
            var totalRowCount = blocks
                .Sum(b => b.RecordCount);
            var builder = new BlockBuilder(materializedBlocks[0].TableSchema, totalRowCount);

            foreach (var block in materializedBlocks)
            {
                builder.AppendBlock(block);
            }

            return builder;
        }
        #endregion

        #region Writable block methods
        public void AppendBlock(IBlock block)
        {
            if (block.TableSchema.TableName != Schema.TableName)
            {
                throw new ArgumentException("Schemas are incompatible", nameof(block));
            }

            if (block is BlockBuilder builder)
            {   //  Optimize for builder appends
                for (var i = 0; i != _dataColumns.Count; ++i)
                {
                    _dataColumns[i].AppendColumn(builder._dataColumns[i]);
                }
            }
            else
            {
                //  Include extra columns
                var data = block.Project(
                    new object?[_dataColumns.Count],
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
            if (record.Length != Schema.Columns.Count)
            {
                throw new ArgumentException(
                    $"Expected {Schema.Columns.Count} columns but is {record.Length}",
                    nameof(record));
            }
            for (int i = 0; i != record.Length; ++i)
            {
                _dataColumns[i].AppendValue(record[i]);
            }
            _dataColumns[Schema.RecordIdColumnIndex]
                .AppendValue(recordId);
        }

        /// <summary>Tries to delete records passed in.</summary>
        /// <param name="recordIds"></param>
        /// <returns>The deleted record IDs.</returns>
        public IEnumerable<long> DeleteRecordsByRecordId(IEnumerable<long> recordIds)
        {
            var recordIdSet = recordIds.ToHashSet();

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

        /// <summary>Clear the entire block.</summary>
        /// <param name="recordIndexes"></param>
        public void Clear()
        {
            foreach (var dataColumn in _dataColumns)
            {
                dataColumn.Clear();
            }
        }
        #endregion

        #region Serialization
        /// <summary>Returns the size serialization size of the entire block.</summary>
        /// <returns></returns>
        public int GetSerializationSize()
        {
            IBlock block = this;
            var totalRecordCount = block.RecordCount;
            var maxRecordCount = totalRecordCount;
            var motherArray = new int[_dataColumns.Count * totalRecordCount];

            var segmentSize = ComputerSegmentSize(
                maxRecordCount,
                0,
                motherArray,
                int.MaxValue);

            return segmentSize.Size;
        }

        /// <summary>
        /// Segments the block's records into batches that can be serialized within a buffer of
        /// size <paramref name="maxBufferLength"/>.
        /// </summary>
        /// <param name="maxBufferLength"></param>
        /// <returns></returns>
        public IReadOnlyList<SegmentSize> SegmentRecords(int maxBufferLength)
        {
            IBlock block = this;
            var totalRecordCount = block.RecordCount;
            var maxRecordCount = totalRecordCount;
            var recordCountList = new List<SegmentSize>();
            var skipRows = 0;
            var motherArray = new int[_dataColumns.Count * totalRecordCount];

            while (skipRows < block.RecordCount)
            {
                var segmentSize = ComputerSegmentSize(
                    maxRecordCount,
                    skipRows,
                    motherArray,
                    maxBufferLength);

                if (segmentSize.ItemCount == 0)
                {
                    throw new InvalidDataException(
                        $"A single record is too large to persist on table " +
                        $"'{block.TableSchema.TableName}' with " +
                        $"{block.TableSchema.Columns.Count} columns");
                }
                //  Cap the amount of records we are going to look at in next iteration to %25 more
                //  than what we did on this round
                maxRecordCount = segmentSize.ItemCount + segmentSize.ItemCount / 4;
                recordCountList.Add(segmentSize);
                skipRows += segmentSize.ItemCount;
            }

            return recordCountList;
        }

        private SegmentSize ComputerSegmentSize(
            int maxRecordCount,
            int skipRows,
            int[] motherArray,
            int maxBufferLength)
        {
            IBlock block = this;
            var recordCount = Math.Min(maxRecordCount, block.RecordCount - skipRows);
            var matchedItemCount = 0;
            var matchedSize = 0;
            var blockHeaderSize =
                sizeof(ushort)  //  Item count
                + _dataColumns.Count * sizeof(ushort);  //  Column payload sizes

            //  Populate motherArray
            for (var j = 0; j != _dataColumns.Count; ++j)
            {
                try
                {
                    var columnsizes = motherArray.AsSpan().Slice(j * recordCount, recordCount);

                    _dataColumns[j].ComputeSerializationSizes(
                        columnsizes,
                        skipRows,
                        maxBufferLength);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(
                        $"Column j={j}, ColumnName={Schema.ColumnProperties[j].ColumnSchema.ColumnName}",
                        ex);
                }
            }
            //  Use motherArray to compute size
            for (var i = 0; i != recordCount; ++i)
            {
                var size = blockHeaderSize;

                for (var j = 0; j != _dataColumns.Count; ++j)
                {
                    size += motherArray[j * recordCount + i];
                }
                if (size > maxBufferLength)
                {
                    return new(matchedItemCount, matchedSize);
                }
                else
                {
                    matchedItemCount = i + 1;
                    matchedSize = size;
                }
            }

            return new(matchedItemCount, matchedSize);
        }

        public BlockStats Serialize(Span<byte> buffer)
        {
#if DEBUG
            var size = GetSerializationSize();

            if (size > buffer.Length)
            {
                throw new InvalidOperationException($"Oversized block ({size}) cannot serialize");
            }
#endif

            var stats = Serialize(buffer, 0, ((IBlock)this).RecordCount);

#if DEBUG
            if (size != stats.Size)
            {
                throw new InvalidOperationException(
                    $"Block size is not estimated properly:  {size} != {stats.Size}");
            }
#endif

            return stats;
        }

        public BlockStats Serialize(Span<byte> buffer, int skipRows, int takeRows)
        {
            IBlock block = this;
            var writer = new ByteWriter(buffer);

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
            var columnsPayloadSizePlaceholder = writer.SliceArrayUInt16(_dataColumns.Count);
            var columnStats = new ColumnStats[_dataColumns.Count];

            //  Body:  columns
            for (var i = 0; i != _dataColumns.Count; ++i)
            {
                var sizeBefore = writer.Position;

                columnStats[i] = _dataColumns[i].SerializeSegment(ref writer, skipRows, takeRows);

                var columnSize = (ushort)(writer.Position - sizeBefore);

                columnsPayloadSizePlaceholder.WriteUInt16(columnSize);
            }

            return new(takeRows, writer.Position, columnStats.ToImmutableArray());
        }
        #endregion

        #region Log
        public NewRecordsContent ToLog()
        {
            var recordCount = RecordCount;
            var newRecordIds = Enumerable.Range(0, recordCount)
                .Select(i => (long)_dataColumns[Schema.RecordIdColumnIndex].GetValue(i)!)
                .ToImmutableList();
            var columns = Enumerable.Range(0, Schema.Columns.Count)
                .Select(i => KeyValuePair.Create(
                    Schema.Columns[i].ColumnName,
                    _dataColumns[i].GetLogValues().ToList()))
                .ToImmutableDictionary();

            return new(newRecordIds, columns);
        }

        public void AppendLog(NewRecordsContent content)
        {
            for (var i = 0; i != Schema.Columns.Count; ++i)
            {
                var columnName = Schema.Columns[i].ColumnName;

                if (content.Columns.ContainsKey(columnName))
                {
                    _dataColumns[i].AppendLogValues(content.Columns[columnName]);
                }
                else
                {   //  There is a column in the schema not present in the logs
                    var nullValue = JsonDocument.Parse("null").RootElement;

                    _dataColumns[i].AppendLogValues(content.NewRecordIds.Select(i => nullValue));
                }
            }
            //  Record ID
            foreach (var newRecordId in content.NewRecordIds)
            {
                _dataColumns[Schema.RecordIdColumnIndex].AppendValue(newRecordId);
            }
        }

        public long? MaxRecordId()
        {
            var recordIdColumn = _dataColumns.Last();

            if (recordIdColumn.RecordCount > 0)
            {
                var maxRecordId = Enumerable.Range(0, recordIdColumn.RecordCount)
                    .Select(i => ((long)recordIdColumn.GetValue(i)!))
                    .Max();

                return maxRecordId;
            }
            else
            {
                return null;
            }
        }
        #endregion
    }
}