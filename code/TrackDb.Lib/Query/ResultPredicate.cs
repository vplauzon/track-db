using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Query
{
    internal class ResultPredicate : IQueryPredicate
    {
        public ResultPredicate(IEnumerable<int> recordIndexes)
        {
            RecordIndexes = recordIndexes.ToImmutableArray();
        }

        public IImmutableList<int> RecordIndexes { get; }

        bool IQueryPredicate.IsTerminal => true;

        IQueryPredicate? IQueryPredicate.FirstPrimitivePredicate => null;

        IQueryPredicate? IQueryPredicate.Simplify(
            Func<IQueryPredicate, IQueryPredicate?> replaceFunc)
        {
            return null;
        }
    }
}