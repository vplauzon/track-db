using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrackDb.Lib.Cache.CachedBlock.SpecializedColumn;
using TrackDb.Lib.Predicate;

namespace TrackDb.Lib.Cache.CachedBlock
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

        #region Constructors
        static ReadOnlyBlockBase()
        {
            DataColumnFactories = CreateDataColumnFactories();
            SupportedDataColumnTypes = DataColumnFactories.Keys
                .ToImmutableHashSet();
        }

        protected ReadOnlyBlockBase(TableSchema schema)
        {
            Schema = schema;
        }

        private static IImmutableDictionary<Type, Func<int, IDataColumn>> CreateDataColumnFactories()
        {
            var builder = ImmutableDictionary<Type, Func<int, IDataColumn>>.Empty.ToBuilder();

            builder.Add(typeof(int), capacity => new ArrayIntColumn(false, capacity));
            builder.Add(typeof(int?), capacity => new ArrayIntColumn(true, capacity));
            builder.Add(typeof(long), capacity => new ArrayLongColumn(false, capacity));
            builder.Add(typeof(long?), capacity => new ArrayLongColumn(true, capacity));
            builder.Add(typeof(string), capacity => new ArrayStringColumn(true, capacity));
            builder.Add(typeof(bool), capacity => new ArrayBoolColumn(false, capacity));
            builder.Add(typeof(bool?), capacity => new ArrayBoolColumn(true, capacity));

            return builder.ToImmutableDictionary();
        }
        #endregion

        public static IImmutableSet<Type> SupportedDataColumnTypes { get; }

        protected static IImmutableDictionary<Type, Func<int, IDataColumn>> DataColumnFactories { get; }

        protected TableSchema Schema { get; }

        protected abstract int RecordCount { get; }

        protected abstract IReadOnlyDataColumn GetDataColumn(int columnIndex);

        #region IBlock
        TableSchema IBlock.TableSchema => Schema;

        int IBlock.RecordCount => RecordCount;

        IEnumerable<int> IBlock.Filter(IQueryPredicate predicate)
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
        private IImmutableSet<int> ResolvePredicate(IQueryPredicate predicate)
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

        private IEnumerable<int> ResolveLeafPredicate(IQueryPredicate leafPredicate)
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