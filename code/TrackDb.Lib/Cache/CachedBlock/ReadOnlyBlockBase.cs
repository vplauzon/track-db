using Ipdb.Lib2.Cache.CachedBlock.SpecializedColumn;
using Ipdb.Lib2.Query;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal abstract class ReadOnlyBlockBase : IBlock
    {
        private readonly object?[] _projectionBuffer;

        #region Constructors
        static ReadOnlyBlockBase()
        {
            DataColumnFactories = CreateDataColumnFactories();
            SupportedDataColumnTypes = DataColumnFactories.Keys
                .ToImmutableHashSet();
        }

        protected ReadOnlyBlockBase(
            TableSchema schema,
            IEnumerable<IReadOnlyDataColumn> dataColumns)
        {
            Schema = schema;
            DataColumns = dataColumns.ToImmutableArray();
            //  Reserve space for record ID + row index
            _projectionBuffer = new object?[Schema.Columns.Count + 2];
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

        protected IImmutableList<IReadOnlyDataColumn> DataColumns { get; }

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
                    .Select(i => (int)i),
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
            IEnumerable<int> rowIndexes,
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