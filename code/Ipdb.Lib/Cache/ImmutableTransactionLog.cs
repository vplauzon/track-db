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
        IImmutableDictionary<TableIndexHash, IImmutableSet<long>> DeletedIndexes)
    {
        public int GetDocumentSize()
        {
            return NewDocuments.Values.Sum(b => b.Length);
        }

        public int GetItemCount()
        {
            return DeletedDocuments.Count
                + NewIndexes.Values.Sum(c => c.Count)
                + DeletedIndexes.Values.Sum(c => c.Count);
        }

        public ImmutableTransactionLog Merge(ImmutableTransactionLog next)
        {
            throw new NotImplementedException();
        }
    }
}