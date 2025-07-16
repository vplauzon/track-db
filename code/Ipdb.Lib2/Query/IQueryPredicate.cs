using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Query
{
    internal interface IQueryPredicate
    {
        /// <summary>Returns the first primitive predicate in the chain.</summary>>
        IQueryPredicate? FirstPrimitivePredicate { get; }
    }
}