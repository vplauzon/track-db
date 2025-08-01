using Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn;
using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class BlockBuilder : ReadOnlyBlockBase, IBlock
    {
        private readonly object?[] _projectionBuffer;

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
            //  Reserve space for record ID + row index
            _projectionBuffer = new object?[Schema.Columns.Count + 2];
        }

        public BlockBuilder(IBlock block)
            : this(block.TableSchema)
        {
            var recordCount = block.RecordCount;
            var data = block.Query(
                new AllInPredicate(),
                //  Include record ID
                Enumerable.Range(0, Schema.Columns.Count + 1));

            //  Copy data
            foreach (var row in data)
            {
                for (var columnIndex = 0; columnIndex != Schema.Columns.Count + 1; ++columnIndex)
                {
                    DataColumns[columnIndex].AppendValue(row.Span[columnIndex]);
                }
            }
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

        public bool IsEmpty => throw new NotImplementedException();

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

        public IEnumerable<long> DeleteRecords(ImmutableList<long> recordIds)
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

        #region IBlock
        TableSchema IBlock.TableSchema => Schema;

        int IBlock.RecordCount => DataColumns.First().RecordCount;

        IEnumerable<ReadOnlyMemory<object?>> IBlock.Query(
            IQueryPredicate predicate,
            IEnumerable<int> projectionColumnIndexes)
        {
            var materializedProjectionColumnIndexes = projectionColumnIndexes.ToImmutableArray();

            if (materializedProjectionColumnIndexes.Count() > _projectionBuffer.Length)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(projectionColumnIndexes),
                    $"{materializedProjectionColumnIndexes.Count()} columns instead of " +
                    $"maximum {_projectionBuffer.Length}");
            }
            //  Initial simplification
            predicate = predicate.Simplify(p => null) ?? predicate;

            while (!predicate.IsTerminal)
            {
                var primitivePredicate = predicate.FirstPrimitivePredicate;

                if (primitivePredicate == null)
                {   //  Should be terminal by now
                    throw new InvalidOperationException("Can't complete query");
                }
                else
                {
                    if (primitivePredicate is BinaryOperatorPredicate binaryOperatorPredicate)
                    {
                        var column = DataColumns[binaryOperatorPredicate.ColumnIndex];
                        var resultIndexes = column.Filter(
                            binaryOperatorPredicate.BinaryOperator,
                            binaryOperatorPredicate.Value);
                        var resultPredicate = new ResultPredicate(resultIndexes);

                        predicate = Simplify(
                            predicate,
                            p => object.ReferenceEquals(p, primitivePredicate)
                            ? resultPredicate
                            : null);
                    }
                    else
                    {
                        throw new NotSupportedException(
                            $"Primitive predicate:  '{primitivePredicate.GetType().Name}'");
                    }
                }
            }
            if (predicate is AllInPredicate)
            {
                return CreateResults(
                    Enumerable.Range(
                        0,
                        DataColumns.First().RecordCount)
                    .Select(i => (short)i),
                    materializedProjectionColumnIndexes);
            }
            else if (predicate is ResultPredicate rp)
            {
                return CreateResults(rp.RecordIndexes, materializedProjectionColumnIndexes);
            }
            else
            {
                throw new NotSupportedException(
                    $"Terminal predicate:  {predicate.GetType().Name}");
            }
        }
        #endregion

        private IQueryPredicate Simplify(
            IQueryPredicate predicate,
            Func<IQueryPredicate, IQueryPredicate?> replaceFunc)
        {
            return replaceFunc(predicate) ?? (predicate.Simplify(replaceFunc) ?? predicate);
        }

        private IEnumerable<ReadOnlyMemory<object?>> CreateResults(
            IEnumerable<short> rowIndexes,
            IImmutableList<int> projectionColumnIndexes)
        {
            var memory = new ReadOnlyMemory<object?>(
                    _projectionBuffer,
                    0,
                    projectionColumnIndexes.Count);

            foreach (var rowIndex in rowIndexes)
            {
                for (var i = 0; i != projectionColumnIndexes.Count; ++i)
                {
                    _projectionBuffer[i] = projectionColumnIndexes[i] < DataColumns.Count
                        ? DataColumns[projectionColumnIndexes[i]].GetValue(rowIndex)
                        : rowIndex;
                }
                yield return memory;
            }
        }
    }
}