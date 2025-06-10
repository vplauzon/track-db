using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Document
{
    internal record DocumentMetaData(
        int TableIndex,
        object PrimaryIndex,
        IReadOnlyCollection<object> SecondaryIndexes);
}