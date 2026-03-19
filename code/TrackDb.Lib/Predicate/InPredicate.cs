using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.Predicate
{
    public sealed record InPredicate<T>(
        int ColumnIndex,
        ISet<T> Values,
        bool HasNullValue,
        bool IsIn)
        : QueryPredicate, IInPredicate
    {
        public InPredicate(int ColumnIndex, IEnumerable<T?> Values, bool IsIn)
            : this(
                  ColumnIndex,
                  Values
                  .Where(v => v != null)
                  .Cast<T>()
                  .ToHashSet(),
                  Values.Any(v => v == null),
                  IsIn)
        {
        }

        #region IInPredicate
        int IInPredicate.ColumnIndex => ColumnIndex;

        QueryPredicate? IInPredicate.InverseIsIn()
        {
            return this with { IsIn = !IsIn };
        }
        #endregion

        internal override IEnumerable<int> ReferencedColumnIndexes => [ColumnIndex];

        internal override IEnumerable<QueryPredicate> LeafPredicates
        {
            get
            {
                yield return this;
            }
        }

        internal override QueryPredicate? Simplify() => Values.Count > 0
            ? null
            : IsIn
            ? ResultPredicate.Empty
            : AllInPredicate.Instance;

        internal override QueryPredicate? Substitute(
            QueryPredicate beforePredicate,
            QueryPredicate afterPredicate)
            => beforePredicate.Equals(this) ? afterPredicate : null;

        internal override QueryPredicate TransformToMetadata(
            IImmutableDictionary<int, MetadataColumnCorrespondance> correspondanceMap)
        {
            if (IsIn)
            {
                if (Values.Count == 0)
                {
                    return ResultPredicate.Empty;
                }
                else if (HasNullValue)
                {   //  Can't do anything with null
                    return AllInPredicate.Instance;
                }
                else
                {
                    var correspondance = correspondanceMap[ColumnIndex];

                    var minMax = Values
                        .Cast<IComparable>()
                        .Aggregate(
                        seed: (Min: (IComparable?)null, Max: (IComparable?)null),
                        func: (acc, val) => (
                        Min: acc.Min == null || val.CompareTo(acc.Min) < 0 ? val : acc.Min,
                        Max: acc.Max == null || val.CompareTo(acc.Max) > 0 ? val : acc.Max
                        ));

                    //  x in set => min_x <= max(set) AND max_x >= min(set)
                    return new ConjunctionPredicate(
                        new BinaryOperatorPredicate(
                            correspondance.MetaMinColumnIndex,
                            minMax.Max,
                            BinaryOperator.LessThanOrEqual),
                        new NegationPredicate(
                            new BinaryOperatorPredicate(
                                correspondance.MetaMaxColumnIndex,
                                minMax.Min,
                                BinaryOperator.LessThan)));
                }
            }
            else
            {
                return AllInPredicate.Instance;
            }
        }

        public override string ToString()
        {
            var set = string.Join(", ", Values.OrderBy(v => v));

            return $"v[{ColumnIndex}] in {{{set}}}";
        }
    }
}