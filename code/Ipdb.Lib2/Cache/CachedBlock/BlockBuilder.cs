using Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn;
using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class BlockBuilder : ReadOnlyBlockBase
    {
        #region Constructors
        public BlockBuilder(TableSchema schema)
            : base(
                  schema,
                  schema.Columns
                  .Select(c => CreateCachedColumn(c.ColumnType, 0))
                  //  Record ID column
                  .Append(new ArrayLongColumn(0)))
        {
            DataColumns = base.DataColumns.Cast<IDataColumn>().ToImmutableArray();
        }

        private static IDataColumn CreateCachedColumn(Type columnType, int capacity)
        {
            if (columnType == typeof(int))
            {
                return new ArrayIntColumn(capacity);
            }
            else if (columnType == typeof(long))
            {
                return new ArrayLongColumn(capacity);
            }
            else
            {
                throw new NotSupportedException($"Column type:  '{columnType}'");
            }
        }
        #endregion

        protected new IImmutableList<IDataColumn> DataColumns { get; }

        #region Writable block methods
        public byte[] Serialize()
        {
            return new byte[10];
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