using Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn;
using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class BlockBuilder : IBlock
    {
        private readonly TableSchema _schema;
        private readonly IImmutableList<ICachedColumn> _dataColumns;

        #region Constructors
        public BlockBuilder(TableSchema schema)
        {
            _schema = schema;
            _dataColumns = _schema.Columns
                .Select(c => CreateCachedColumn(c.ColumnType, Array.Empty<object>()))
                //  Record ID column
                .Append(new ArrayLongColumn(Array.Empty<object>()))
                .ToImmutableArray();
        }

        public BlockBuilder(IBlock block)
        {
            _schema = block.TableSchema;
            _dataColumns = Enumerable.Range(0, _schema.Columns.Count)
                .Select(i => CreateCachedColumn(
                    _schema.Columns[i].ColumnType,
                    block.GetColumnData(i)))
                .Append(CreateCachedColumn(typeof(long), block.GetColumnData(_schema.Columns.Count)))
                .ToImmutableArray();
        }

        private static ICachedColumn CreateCachedColumn(
            Type columnType,
            IEnumerable<object?> data)
        {
            if (columnType == typeof(int))
            {
                return new ArrayIntColumn(data);
            }
            else if (columnType == typeof(long))
            {
                return new ArrayLongColumn(data);
            }
            else
            {
                throw new NotSupportedException($"Column type:  '{columnType}'");
            }
        }
        #endregion

        public bool IsEmpty => throw new NotImplementedException();

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

        public IEnumerable<long> DeleteRecords(ImmutableList<long> recordIds)
        {
            var recordIdSet = recordIds.ToImmutableHashSet();

            if (recordIdSet.Any() && _dataColumns.First().RecordCount > 0)
            {
                var columns = new object?[_schema.Columns.Count];
                var recordIdColumn = (ArrayLongColumn)_dataColumns.Last();
                var deletedRecordPairs = Enumerable.Range(0, _dataColumns.First().RecordCount)
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

                    foreach (var dataColumn in _dataColumns)
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
        TableSchema IBlock.TableSchema => _schema;

        int IBlock.RecordCount => _dataColumns.First().RecordCount;

        IEnumerable<long> IBlock.RecordIds => ((ArrayLongColumn)_dataColumns.Last()).EnumerableRawData;

        IEnumerable<object?> IBlock.GetColumnData(int columnIndex)
        {
            var column = _dataColumns[columnIndex];

            return Enumerable.Range(0, _dataColumns.First().RecordCount)
                .Select(i => column.GetData((short)i));
        }

        IEnumerable<QueryResult> IBlock.Query(
            IQueryPredicate predicate,
            IImmutableList<int> projectionColumnIndexes)
        {
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
                        var column = _dataColumns[binaryOperatorPredicate.ColumnIndex];
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
            var projectionColumns = projectionColumnIndexes
                .Select(i => _dataColumns[i])
                .ToImmutableArray();

            if (predicate is AllInPredicate)
            {
                return CreateResults(
                    Enumerable.Range(
                        0,
                        _dataColumns.First().RecordCount)
                    .Select(i => (short)i),
                    projectionColumns);
            }
            else if (predicate is ResultPredicate rp)
            {
                return CreateResults(rp.RecordIndexes, projectionColumns);
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

        private IEnumerable<QueryResult> CreateResults(
            IEnumerable<short> rowIndexes,
            IImmutableList<ICachedColumn> projectionColumns)
        {
            var projectionBuffer = new object?[projectionColumns.Count];
            var recordIdColumn = (ArrayLongColumn)_dataColumns.Last();
            var results = rowIndexes
                .Select(i => new QueryResult(
                    recordIdColumn.RawData[i],
                    () =>
                    {
                        CreateRow(projectionBuffer, i, projectionColumns);

                        return projectionBuffer;
                    }));

            return results;
        }

        private void CreateRow(
            object?[] buffer,
            short rowIndex,
            IImmutableList<ICachedColumn> projectionColumns)
        {
            for (int i = 0; i != projectionColumns.Count; ++i)
            {
                buffer[i] = projectionColumns[i].GetData(rowIndex);
            }
        }
    }
}