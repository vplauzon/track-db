using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Cache
{
    internal record TableIndexHash(TableIndexKey TableIndexKey, long IndexHash);
}