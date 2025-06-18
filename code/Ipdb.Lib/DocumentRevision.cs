using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib
{
    internal record DocumentRevision<T>(long RevisionId, T Document);
}
