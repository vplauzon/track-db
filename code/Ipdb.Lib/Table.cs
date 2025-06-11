using Ipdb.Lib.Querying;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;

namespace Ipdb.Lib
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class Table<T>
    {
        private readonly int _tableIndex;
        private readonly TableSchema<T> _schema;
        private readonly DataManager _storageManager;

        internal Table(int tableIndex, TableSchema<T> schema, DataManager storageManager)
        {
            _tableIndex = tableIndex;
            _schema = schema;
            _storageManager = storageManager;
        }

        public QueryOp<T> QueryOp { get; } = new QueryOp<T>();

        public IEnumerable<T> Query(QueryPredicate<T> predicate)
        {
            throw new NotSupportedException(
                "Only predicates to primary or secondary indexes are supported");
        }

        public void AppendDocuments(params IEnumerable<T> documents)
        {
            foreach (var document in documents)
            {
                if (document == null)
                {
                    throw new ArgumentNullException(nameof(documents));
                }

                //  Persist the document itself
                var revisionId = _storageManager.DocumentManager.AppendDocument(
                    document);
                var indexHashes = _schema.Indexes
                    .Select(i => i.ObjectExtractor(document))
                    .Select(v => v.Hash)
                    .ToImmutableArray();

                for (int i = 0; i != indexHashes.Length; ++i)
                {
                    var v1 = _tableIndex;
                    var v2 = i;
                    var v3 = indexHashes[i];
                    var v4 = revisionId;

                    _storageManager.IndexManager.AppendIndex(
                            _tableIndex,
                            i,
                            indexHashes[i],
                            revisionId);
                }
            }
        }

        public long DeleteDocuments(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }
    }
}
