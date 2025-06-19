using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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

        public TransactionLog ToTransactionLog()
        {
            var logs = new TransactionLog(
                NewDocuments.ToImmutableDictionary().ToBuilder(),
                DeletedDocuments.ToImmutableHashSet().ToBuilder(),
                NewIndexes.ToImmutableDictionary(
                    p => p.Key,
                    p => p.Value.ToImmutableHashSet().ToBuilder()).ToBuilder(),
                DeletedIndexes.ToImmutableDictionary(
                    p => p.Key,
                    p => p.Value.ToImmutableHashSet().ToBuilder()).ToBuilder());

            return logs;
        }

        public ImmutableTransactionLog Merge(ImmutableTransactionLog next)
        {
            var log = ToTransactionLog();

            foreach (var pair in next.NewDocuments)
            {
                log.AppendDocument(pair.Key, pair.Value);
            }
            foreach (var revisionId in next.DeletedDocuments)
            {
                log.DeleteDocument(revisionId);
            }
            foreach (var pair in next.NewIndexes)
            {
                foreach (var hash in pair.Value)
                {
                    log.AppendIndexValue(pair.Key, hash);
                }
            }
            foreach (var pair in next.DeletedIndexes)
            {
                foreach (var hash in pair.Value)
                {
                    log.DeleteIndexValue(pair.Key, hash);
                }
            }

            return log.ToImmutable();
        }
    }
}