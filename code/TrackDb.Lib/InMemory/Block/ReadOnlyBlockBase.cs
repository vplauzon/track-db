using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.InMemory.Block.SpecializedColumn;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.InMemory.Block
{
    internal abstract class ReadOnlyBlockBase : IBlock
    {
        #region Inner types
        private class RowIndexColumn : IReadOnlyDataColumn
        {
            int IReadOnlyDataColumn.RecordCount => throw new NotSupportedException();

            IEnumerable<int> IReadOnlyDataColumn.FilterBinary(BinaryOperator binaryOperator, object? value)
            {
                throw new NotSupportedException();
            }

            IEnumerable<int> IReadOnlyDataColumn.FilterIn(IImmutableSet<object?> values)
            {
                throw new NotSupportedException();
            }

            object? IReadOnlyDataColumn.GetValue(int index)
            {
                return index;
            }
        }

        private record BlockIdColumn(int BlockId) : IReadOnlyDataColumn
        {
            public int RecordCount => throw new NotSupportedException();

            public IEnumerable<int> FilterBinary(BinaryOperator binaryOperator, object? value)
            {
                throw new NotSupportedException();
            }

            public IEnumerable<int> FilterIn(IImmutableSet<object?> values)
            {
                throw new NotSupportedException();
            }

            public object? GetValue(int index)
            {
                return BlockId;
            }
        }
        #endregion

        private static readonly IImmutableDictionary<Type, Func<int, IDataColumn>> _dataFactoryMap;

        #region Constructors
        static ReadOnlyBlockBase()
        {
            var builder = ImmutableDictionary<Type, Func<int, IDataColumn>>.Empty.ToBuilder();

            builder.Add(typeof(short), capacity => new ArrayShortColumn(false, capacity));
            builder.Add(typeof(short?), capacity => new ArrayShortColumn(true, capacity));
            builder.Add(typeof(int), capacity => new ArrayIntColumn(false, capacity));
            builder.Add(typeof(int?), capacity => new ArrayIntColumn(true, capacity));
            builder.Add(typeof(long), capacity => new ArrayLongColumn(false, capacity));
            builder.Add(typeof(long?), capacity => new ArrayLongColumn(true, capacity));
            builder.Add(typeof(string), capacity => new ArrayStringColumn(true, capacity));
            builder.Add(typeof(bool), capacity => new ArrayBoolColumn(false, capacity));
            builder.Add(typeof(bool?), capacity => new ArrayBoolColumn(true, capacity));
            builder.Add(typeof(DateTime), capacity => new ArrayDateTimeColumn(false, capacity));
            builder.Add(typeof(DateTime?), capacity => new ArrayDateTimeColumn(true, capacity));
            builder.Add(typeof(Uri), capacity => new ArrayUriColumn(true, capacity));

            _dataFactoryMap = builder.ToImmutable();
        }

        protected ReadOnlyBlockBase(TableSchema schema)
        {
            Schema = schema;
        }
        #endregion

        public static bool IsSupportedDataColumnType(Type type)
        {
            return _dataFactoryMap.ContainsKey(type)
                || (type.IsEnum && type.GetEnumUnderlyingType() == typeof(int))
                || (type.IsGenericType
                && type.GetGenericTypeDefinition() == typeof(Nullable<>)
                && type.GenericTypeArguments.Length == 1
                && type.GenericTypeArguments[0].IsEnum
                && type.GenericTypeArguments[0].GetEnumUnderlyingType() == typeof(int));
        }

        protected static IDataColumn CreateDataColumn(Type columnType, int capacity)
        {
            IDataColumn CreateEnumDataColumn(
                Type columnType,
                bool allowNulls,
                int capacity)
            {
                var dataColumnType = typeof(ArrayEnumColumn<>).MakeGenericType(columnType);
                var dataColumn = Activator.CreateInstance(
                    dataColumnType,
                    BindingFlags.Instance | BindingFlags.Public,
                    null,
                    [allowNulls, capacity],
                    null);

                return (IDataColumn)dataColumn!;
            }

            if (_dataFactoryMap.TryGetValue(columnType, out var factory))
            {
                return factory(capacity);
            }
            else if (columnType.IsEnum && columnType.GetEnumUnderlyingType() == typeof(int))
            {
                return CreateEnumDataColumn(columnType, false, capacity);
            }
            else if (columnType.IsGenericType
                && columnType.GetGenericTypeDefinition() == typeof(Nullable<>)
                && columnType.GenericTypeArguments.Length == 1
                && columnType.GenericTypeArguments[0].IsEnum
                && columnType.GenericTypeArguments[0].GetEnumUnderlyingType() == typeof(int))
            {
                return CreateEnumDataColumn(columnType.GenericTypeArguments[0], true, capacity);
            }
            else
            {
                throw new NotSupportedException($"Column type:  '{columnType}'");
            }
        }

        protected TableSchema Schema { get; }

        protected abstract int RecordCount { get; }

        protected abstract IReadOnlyDataColumn GetDataColumn(int columnIndex);

        #region IBlock
        TableSchema IBlock.TableSchema => Schema;

        int IBlock.RecordCount => RecordCount;

        IEnumerable<int> IBlock.Filter(QueryPredicate predicate)
        {
            //  Initiate a simplification prior to the resolution process
            var resultRowIndexes = ResolvePredicate(predicate.Simplify() ?? predicate);

            return resultRowIndexes;
        }

        IEnumerable<ReadOnlyMemory<object?>> IBlock.Project(
            Memory<object?> buffer,
            IEnumerable<int> projectionColumnIndexes,
            IEnumerable<int> rowIndexes,
            int blockId)
        {
            var materializedProjectionColumnIndexes = projectionColumnIndexes.ToImmutableArray();

            if (materializedProjectionColumnIndexes.Count() != buffer.Length)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(projectionColumnIndexes),
                    $"Number of projected columns ({materializedProjectionColumnIndexes.Count()}) " +
                    $"isn't the same as the buffer size ({buffer.Length})");
            }

            var columns = materializedProjectionColumnIndexes
                .Select(index => index < 0 || index > Schema.Columns.Count + 2
                ? throw new ArgumentOutOfRangeException(
                    nameof(projectionColumnIndexes),
                    $"Column '{index}' is out-of-range")
                : index <= Schema.Columns.Count
                ? GetDataColumn(index)
                : index == Schema.Columns.Count + 1
                ? new RowIndexColumn()
                : new BlockIdColumn(blockId))
                .ToImmutableArray();

            foreach (var rowIndex in rowIndexes)
            {
                for (var i = 0; i != columns.Length; ++i)
                {
                    buffer.Span[i] = columns[i].GetValue(rowIndex);
                }
                yield return buffer;
            }
        }
        #endregion

        #region Predicate filtering
        private IImmutableSet<int> ResolvePredicate(QueryPredicate predicate)
        {
            var leafPredicate = predicate.LeafPredicates.FirstOrDefault();

            if (leafPredicate != null)
            {
                var resultPredicate = new ResultPredicate(ResolveLeafPredicate(leafPredicate));
                var newPredicate = predicate.Substitute(leafPredicate, resultPredicate);

                if (newPredicate == null)
                {
                    throw new InvalidOperationException("Predicate substitution failed");
                }
                else
                {
                    return ResolvePredicate(newPredicate.Simplify() ?? newPredicate);
                }
            }
            else if (predicate is ResultPredicate rp)
            {
                return rp.RecordIndexes;
            }
            else
            {
                var finalSubstitution = predicate.Substitute(
                    AllInPredicate.Instance,
                    new ResultPredicate(Enumerable.Range(0, RecordCount)));

                if (finalSubstitution == null)
                {
                    throw new InvalidOperationException("Final substitution failed");
                }
                else
                {
                    var finalPredicate = finalSubstitution.Simplify() ?? finalSubstitution;

                    if (finalPredicate is ResultPredicate rp2)
                    {
                        return rp2.RecordIndexes;
                    }
                    else
                    {
                        throw new InvalidOperationException("Can't complete query");
                    }
                }
            }
        }

        private IEnumerable<int> ResolveLeafPredicate(QueryPredicate leafPredicate)
        {
            if (leafPredicate is BinaryOperatorPredicate bop)
            {
                var column = GetDataColumn(bop.ColumnIndex);
                var resultIndexes = column.FilterBinary(bop.BinaryOperator, bop.Value);

                return resultIndexes;
            }
            else if (leafPredicate is InPredicate ip)
            {
                var column = GetDataColumn(ip.ColumnIndex);
                var resultIndexes = column.FilterIn(ip.Values);

                return resultIndexes;
            }
            else
            {
                throw new NotSupportedException(
                    $"Primitive predicate:  '{leafPredicate.GetType().Name}'");
            }
        }
        #endregion
    }
}