using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal record TypedQueryPredicateAdapter<T>(IQueryPredicate Adaptee)
        : ITypedQueryPredicate<T>
        where T : notnull
    {
        bool IEquatable<IQueryPredicate>.Equals(IQueryPredicate? other)
        {
            return other is TypedQueryPredicateAdapter<T> tqpa
                && tqpa.Adaptee.Equals(Adaptee);
        }

        IEnumerable<IQueryPredicate> IQueryPredicate.LeafPredicates => Adaptee.LeafPredicates;

        IQueryPredicate? IQueryPredicate.Simplify()
        {
            return Adaptee.Simplify() ?? Adaptee;
        }

        IQueryPredicate? IQueryPredicate.Substitute(
            IQueryPredicate beforePredicate,
            IQueryPredicate afterPredicate)
        {
            var ads = Adaptee.Substitute(beforePredicate, afterPredicate);

            return ads != null
                ? new TypedQueryPredicateAdapter<T>(ads)
                : null;
        }

        public override string ToString()
        {
            return Adaptee.ToString()!;
        }
    }
}