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
        private readonly TableSchema<T> _schema;
        private readonly ImmutableDictionary<string, IndexDefinition<T>> _indexByPath;
        private readonly DataManager _storageManager;

        internal Table(TableSchema<T> schema, DataManager storageManager)
        {
            _schema = schema;
            _indexByPath = _schema.Indexes.ToImmutableDictionary(i => i.PropertyPath);
            _storageManager = storageManager;
            QueryOp = new QueryOp<T>(_schema.Indexes
                .ToImmutableDictionary(i => i.PropertyPath, i => i));
        }

        public QueryOp<T> QueryOp { get; }

        public IImmutableList<T> Query(PredicateBase<T> predicate)
        {
            while (true)
            {
                var primitivePredicate = predicate.FirstPrimitivePredicate;

                if (primitivePredicate == null)
                {
                    return QueryResult(predicate);
                }
                else if (primitivePredicate is IIndexEqual<T> ie)
                {
                    var revisionIds = _storageManager.IndexManager.FindEqualHash(
                        _schema.TableName,
                        ie.IndexDefinition.PropertyPath,
                        ie.KeyHash);

                    predicate = predicate.Simplify(primitivePredicate, revisionIds)
                        ?? throw new InvalidOperationException("Predicate should simplify");
                }
                else
                {
                    throw new NotSupportedException(
                        $"Primitive '{primitivePredicate.GetType().Name}'");
                }
            }
        }

        private IImmutableList<T> QueryResult(PredicateBase<T> resultPredicate)
        {
            if (resultPredicate is ResultPredicate<T> rp)
            {
                //rp.RevisionIds;
                throw new NotImplementedException();
            }
            else
            {
                throw new NotSupportedException(
                    $"Result predicate:  '{resultPredicate.GetType().Name}'");
            }
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

                foreach (var index in _schema.Indexes)
                {
                    _storageManager.IndexManager.AppendIndex(
                        _schema.TableName,
                        index.PropertyPath,
                        index.HashExtractor(document),
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
