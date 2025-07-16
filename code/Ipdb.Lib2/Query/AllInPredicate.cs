using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Query
{
    internal class AllInPredicate : IQueryPredicate
    {
        bool IQueryPredicate.IsTerminal => true;

        IQueryPredicate? IQueryPredicate.FirstPrimitivePredicate => null;

        IQueryPredicate? IQueryPredicate.Simplify(
            Func<IQueryPredicate, IQueryPredicate?> replaceFunc) => null;
    }
}