using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrackDb.Lib.Predicate
{
    internal record BinaryOperatorPredicate(
        int ColumnIndex,
        object? Value,
        BinaryOperator BinaryOperator)
        : IQueryPredicate
    {
        bool IQueryPredicate.IsTerminal => false;

        IQueryPredicate? IQueryPredicate.FirstPrimitivePredicate => this;

        IQueryPredicate? IQueryPredicate.Simplify(
            Func<IQueryPredicate, IQueryPredicate?> replaceFunc) => null;
    }
}