using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib.Predicate
{
    public sealed record InPredicate(
        int ColumnIndex,
        ISet<object?> Values,
        bool IsIn)
        : QueryPredicate
    {
        public InPredicate(int ColumnIndex, IEnumerable<object?> Values, bool IsIn)
            : this(ColumnIndex, Values.ToHashSet(), IsIn)
        {
        }

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
            : ResultPredicate.Empty;

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
                else if (Values.Contains(null))
                {
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