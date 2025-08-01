using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal abstract class ReadOnlyBlockBase
    {
        protected ReadOnlyBlockBase(TableSchema schema)
        {
            Schema = schema;
        }

        protected TableSchema Schema { get; }
    }
}