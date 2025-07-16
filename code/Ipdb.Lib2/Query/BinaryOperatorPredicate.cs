using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Query
{
    internal record BinaryOperatorPredicate(
        string PropertyPath,
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