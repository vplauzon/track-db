using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ipdb.Lib.Cache
{
    internal record ImmutableTransactionLog(
        IImmutableDictionary<long, byte[]> NewDocuments,
        IImmutableSet<long> DeletedDocuments,
        IImmutableDictionary<TableIndexHash, IImmutableSet<long>> NewIndexes,
        IImmutableDictionary<TableIndexHash, IImmutableSet<long>> DeletedIndexes);
}