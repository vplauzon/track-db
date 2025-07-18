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
        private readonly ICachedColumn _recordIdColumn;
        private readonly IImmutableList<ICachedColumn> _dataColumns;
        private readonly object[] _dataColumnBuffer;

        #region Constructors
        public BlockBuilder(TableSchema schema)
        {
            _schema = schema;
            _recordIdColumn = CreateRecordIdColumn(Array.Empty<long>());
            _dataColumns = _schema.Columns
                .Select(c => CreateCachedColumn(c.ColumnType, Array.Empty<object>()))
                .ToImmutableArray();
            _dataColumnBuffer = new object[_schema.Columns.Count];
        }

        public BlockBuilder(IBlock block)
        {
            _schema = block.TableSchema;
            _recordIdColumn = CreateRecordIdColumn(block.RecordIds);
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

        private static ICachedColumn CreateRecordIdColumn(IEnumerable<long> recordIds)
        {
            return new ArrayLongColumn(recordIds.Cast<object>());
        }
        #endregion

        public bool IsEmpty => throw new NotImplementedException();

        public void AppendRecord(long recordId, object record)
        {
            _recordIdColumn.AppendValue(recordId);
            _schema.FromObjectToColumns(record, _dataColumnBuffer);
        }

        #region IBlock
        TableSchema IBlock.TableSchema => _schema;

        int IBlock.RecordCount => _recordIdColumn.RecordCount;

        IEnumerable<long> IBlock.RecordIds => _recordIdColumn.Data.Cast<long>();

        IEnumerable<object?> IBlock.GetColumnData(int columnIndex)
        {
            return _dataColumns[columnIndex].Data;
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
                return _recordIdColumn.Data.Cast<long>().ToImmutableArray();
            }
            else if (predicate is ResultPredicate rp)
            {
                return _recordIdColumn.Data
                    .Cast<long>()
                    .ToImmutableArray();
            }
            else
            {
                throw new NotSupportedException(
                    $"Terminal predicate:  {predicate.GetType().Name}");
            }
        }

        IEnumerable<object> IBlock.GetRecords(IEnumerable<long> recordIds)
        {
            throw new NotImplementedException();
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