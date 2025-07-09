using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal interface ICachedColumn
    {
        int RecordCount { get; }

        IEnumerable<object> Data { get; }

        void AppendValue(object? value);
    }
}