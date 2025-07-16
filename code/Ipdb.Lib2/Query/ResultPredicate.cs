using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Query
{
    internal class ResultPredicate : IQueryPredicate
    {
        IQueryPredicate? IQueryPredicate.FirstPrimitivePredicate => null;
    }
}