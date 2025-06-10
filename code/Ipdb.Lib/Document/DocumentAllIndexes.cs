using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Document
{
    internal record DocumentAllIndexes(
        object Primary,
        IReadOnlyCollection<object> Secondaries,
        FilePosition DocumentPosition);
}