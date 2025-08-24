using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    public interface IQueryPredicate
    {
        /// <summary>
        /// <c>true</c> iif this predicate is terminal, i.e. completely simplified.
        /// </summary>
        bool IsTerminal { get; }

        /// <summary>Returns the first primitive predicate in the chain.</summary>>
        IQueryPredicate? FirstPrimitivePredicate { get; }

        /// <summary>
        /// Simplifies the predicate by running a replace function on each node.
        /// </summary>>
        /// <param name="replaceFunc">Function replacing a predicate.</param>
        /// <returns>Simplified predicate (or <c>null</c> if unchanged).</returns>
        IQueryPredicate? Simplify(Func<IQueryPredicate, IQueryPredicate?> replaceFunc);
    }
}