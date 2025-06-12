using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Querying
{
    internal record HashMatch(Expression Expression, IImmutableList<short> Hashes);
}