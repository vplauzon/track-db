using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib2.Cache.CachedBlock
{
    internal interface IDataColumn : IReadOnlyDataColumn
    {
        /// <summary>Append value at the end of the column.</summary>
        /// <param name="value"></param>
        void AppendValue(object? value);

        /// <summary>Delete record indexes.</summary>
        /// <param name="recordIndexes">Assumed to be in increasing order.</param>
        void DeleteRecords(IEnumerable<short> recordIndexes);
    }
}