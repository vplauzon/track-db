using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal class AllInPredicate : IQueryPredicate
    {
        private AllInPredicate()
        {
        }

        public static AllInPredicate Instance { get; } = new AllInPredicate();

        bool IQueryPredicate.IsTerminal => true;

        IQueryPredicate? IQueryPredicate.FirstPrimitivePredicate => null;

        IQueryPredicate? IQueryPredicate.Simplify(
            Func<IQueryPredicate, IQueryPredicate?> replaceFunc) => null;
    }
}