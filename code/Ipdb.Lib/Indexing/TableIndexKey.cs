using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Indexing
{
    internal record TableIndexKey(string TableName, string IndexName);
}