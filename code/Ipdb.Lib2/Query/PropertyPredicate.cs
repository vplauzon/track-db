using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Query
{
    internal record PropertyPredicate(
        string PropertyPath,
        object? Value,
        BinaryOperator BinaryOperator)
        : IQueryPredicate
    {
    }
}