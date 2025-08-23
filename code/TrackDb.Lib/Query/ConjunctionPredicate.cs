using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Query
{
    internal record ConjunctionPredicate(
        IQueryPredicate LeftPredicate,
        IQueryPredicate RightPredicate)
        : IQueryPredicate
    {
        bool IQueryPredicate.IsTerminal => false;

        IQueryPredicate? IQueryPredicate.FirstPrimitivePredicate => null;

        IQueryPredicate? IQueryPredicate.Simplify(
            Func<IQueryPredicate, IQueryPredicate?> replaceFunc)
        {
            var newLeftPredicate = replaceFunc(LeftPredicate);
            var newRightPredicate = replaceFunc(RightPredicate);
            var resultingLeftPredicate = newLeftPredicate ?? LeftPredicate;
            var resultingRightPredicate = newRightPredicate ?? RightPredicate;

            if (resultingLeftPredicate is AllInPredicate)
            {
                return resultingRightPredicate;
            }
            else if (resultingRightPredicate is AllInPredicate)
            {
                return resultingLeftPredicate;
            }
            else
            {
                return newLeftPredicate == null && newRightPredicate == null
                    ? this
                    : new ConjunctionPredicate(resultingLeftPredicate, resultingRightPredicate);
            }
        }
    }
}