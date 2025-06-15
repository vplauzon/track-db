using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Ipdb.Lib.Cache
{
    internal record TransactionLog(
        ImmutableDictionary<long, byte[]>.Builder NewDocuments,
        ImmutableHashSet<long>.Builder DeletedDocuments,
        ImmutableDictionary<TableIndexHash, ImmutableHashSet<long>.Builder>.Builder NewIndexes,
        ImmutableDictionary<TableIndexHash, ImmutableHashSet<long>.Builder>.Builder DeletedIndexes)
    {
        public TransactionLog()
            : this(
                  ImmutableDictionary<long, byte[]>.Empty.ToBuilder(),
                  ImmutableHashSet<long>.Empty.ToBuilder(),
                  ImmutableDictionary<TableIndexHash, ImmutableHashSet<long>.Builder>.Empty.ToBuilder(),
                  ImmutableDictionary<TableIndexHash, ImmutableHashSet<long>.Builder>.Empty.ToBuilder())
        {
        }

        public ImmutableTransactionLog ToImmutable(long transactionId)
        {
            return new ImmutableTransactionLog(
                transactionId,
                NewDocuments.ToImmutable(),
                DeletedDocuments.ToImmutable(),
                NewIndexes.ToImmutableDictionary(
                    k => k.Key,
                    v => (IImmutableSet<long>)v.Value.ToImmutable()),
                DeletedIndexes.ToImmutableDictionary(
                    k => k.Key,
                    v => (IImmutableSet<long>)v.Value.ToImmutable()));
        }

        #region Data Manipulation
        public void AddDocument(long revisionId, byte[] Payload)
        {
            NewDocuments.Add(revisionId, Payload);
        }

        public void DeleteDocument(long revisionId)
        {
            if (NewDocuments.ContainsKey(revisionId))
            {
                NewDocuments.Remove(revisionId);
            }
            DeletedDocuments.Add(revisionId);
        }

        public void AddIndexValue(TableIndexHash tableIndexHash, long revisionId)
        {
            if (!NewIndexes.TryGetValue(tableIndexHash, out var revisionIds))
            {
                revisionIds = ImmutableHashSet<long>.Empty.ToBuilder();
                NewIndexes.Add(tableIndexHash, revisionIds);
            }
            revisionIds.Add(revisionId);
        }

        public void DeleteIndexValue(TableIndexHash tableIndexHash, long revisionId)
        {
            if (NewIndexes.TryGetValue(tableIndexHash, out var newRevisionIds)
                && newRevisionIds.Contains(revisionId))
            {
                newRevisionIds.Remove(revisionId);
            }
            else
            {
                if (!DeletedIndexes.TryGetValue(tableIndexHash, out var deletedRevisionIds))
                {
                    deletedRevisionIds = ImmutableHashSet<long>.Empty.ToBuilder();
                    DeletedIndexes.Add(tableIndexHash, deletedRevisionIds);
                }
                deletedRevisionIds.Add(revisionId);
            }
        }
        #endregion
    }
}