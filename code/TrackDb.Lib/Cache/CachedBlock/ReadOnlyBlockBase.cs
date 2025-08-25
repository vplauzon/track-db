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

            var results = ResolvePredicate(predicate);

            return CreateResults(results, materializedProjectionColumnIndexes);
        }
        #endregion

        private IImmutableSet<int> ResolvePredicate(IQueryPredicate predicate)
        {
            predicate = predicate.Simplify() ?? predicate;

            var leafPredicate = predicate.LeafPredicates.FirstOrDefault();

            if (leafPredicate != null)
            {
                var newPredicate =
                    predicate.Substitute(leafPredicate, ResolveLeafPredicate(leafPredicate));

                if (newPredicate == null)
                {
                    throw new InvalidOperationException("Predicate substitution failed");
                }
                else
                {
                    return ResolvePredicate(newPredicate);
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
                    new ResultPredicate(
                        Enumerable.Range(
                            0,
                            DataColumns.First().RecordCount)));

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

        private IQueryPredicate ResolveLeafPredicate(IQueryPredicate leafPredicate)
        {
            if (leafPredicate is BinaryOperatorPredicate binaryOperatorPredicate)
            {
                var column = DataColumns[binaryOperatorPredicate.ColumnIndex];
                var resultIndexes = column.Filter(
                    binaryOperatorPredicate.BinaryOperator,
                    binaryOperatorPredicate.Value);
                var resultPredicate = new ResultPredicate(resultIndexes);

                return resultPredicate;
            }
            else
            {
                throw new NotSupportedException(
                    $"Primitive predicate:  '{leafPredicate.GetType().Name}'");
            }
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