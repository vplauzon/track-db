using Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn;
using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal class BlockBuilder : IBlock
    {
        private readonly TableSchema _schema;
        private readonly ArrayLongColumn _recordIdColumn;
        private readonly IImmutableList<ICachedColumn> _dataColumns;
        private readonly object[] _dataColumnBuffer;

        #region Constructors
        public BlockBuilder(TableSchema schema)
        {
            _schema = schema;
            _recordIdColumn = new ArrayLongColumn(Array.Empty<object>());
            _dataColumns = _schema.Columns
                .Select(c => CreateCachedColumn(c.ColumnType, Array.Empty<object>()))
                .ToImmutableArray();
            _dataColumnBuffer = new object[_schema.Columns.Count];
        }

        public BlockBuilder(IBlock block)
        {
            _schema = block.TableSchema;
            _recordIdColumn = new ArrayLongColumn(block.RecordIds.Cast<object>());
            _dataColumns = Enumerable.Range(0, _schema.Columns.Count)
                .Select(i => CreateCachedColumn(
                    _schema.Columns[i].ColumnType,
                    block.GetColumnData(i)))
                .ToImmutableArray();
            _dataColumnBuffer = new object[_schema.Columns.Count];
        }

        private static ICachedColumn CreateCachedColumn(
            Type columnType,
            IEnumerable<object?> data)
        {
            if (columnType == typeof(int))
            {
                return new ArrayIntColumn(data);
            }
            else
            {
                throw new NotSupportedException($"Column type:  '{columnType}'");
            }
        }
        #endregion

        public bool IsEmpty => throw new NotImplementedException();

        public void AppendRecord(long recordId, object record)
        {
            ((ICachedColumn)_recordIdColumn).AppendValue(recordId);
            _schema.FromObjectToColumns(record, _dataColumnBuffer);
            for (int i = 0; i != _dataColumns.Count(); ++i)
            {
                _dataColumns[i].AppendValue(_dataColumnBuffer[i]);
            }
        }

        #region IBlock
        TableSchema IBlock.TableSchema => _schema;

        int IBlock.RecordCount => _recordIdColumn.RawData.Length;

        IEnumerable<long> IBlock.RecordIds => _recordIdColumn.EnumerableRawData;

        IEnumerable<object?> IBlock.GetColumnData(int columnIndex)
        {
            var column = _dataColumns[columnIndex];

            return Enumerable.Range(0, _recordIdColumn.RawData.Length)
                .Select(i => column.GetData((short)i));
        }

        IEnumerable<long> IBlock.Query(IQueryPredicate predicate)
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
                        if (_schema.TryGetColumnIndex(
                            binaryOperatorPredicate.PropertyPath,
                            out var columnIndex))
                        {
                            var column = _dataColumns[columnIndex];
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
                            throw new InvalidOperationException(
                                $"Unknown property path:  " +
                                $"'{binaryOperatorPredicate.PropertyPath}'");
                        }
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
                return _recordIdColumn.EnumerableRawData;
            }
            else if (predicate is ResultPredicate rp)
            {
                return rp.RecordIndexes
                    .Select(i => _recordIdColumn.RawData[i]);
            }
            else
            {
                throw new NotSupportedException(
                    $"Terminal predicate:  {predicate.GetType().Name}");
            }
        }

        IEnumerable<object> IBlock.GetRecords(IEnumerable<long> recordIds)
        {
            var columns = new object?[_schema.Columns.Count];

            foreach (var recordId in recordIds)
            {
                var recordIndex = (short)_recordIdColumn.RawData.IndexOf(recordId);

                if (recordIndex == -1)
                {
                    throw new ArgumentOutOfRangeException(
                        $"Record ID {recordId} not found in block");
                }

                // Create record object from columns at recordIndex
                for (int i = 0; i < _schema.Columns.Count; i++)
                {
                    columns[i] = _dataColumns[i].GetData(recordIndex)!;
                }

                yield return _schema.FromColumnsToObject(columns);
            }
        }

        private IQueryPredicate Simplify(
            IQueryPredicate predicate,
            Func<IQueryPredicate, IQueryPredicate?> replaceFunc)
        {
            return replaceFunc(predicate) ?? (predicate.Simplify(replaceFunc) ?? predicate);
        }
        #endregion
    }
}
