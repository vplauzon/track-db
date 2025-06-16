using Ipdb.Lib.Cache;
using Ipdb.Lib.Querying;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Text.Json;

namespace Ipdb.Lib
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.NonPublicConstructors)]
    public class Table<T>
    {
        private readonly TableSchema<T> _schema;
        private readonly ImmutableDictionary<string, IndexDefinition<T>> _indexByPath;
        private readonly IDatabaseService _databaseService;

        internal Table(TableSchema<T> schema, IDatabaseService databaseService)
        {
            _schema = schema;
            _indexByPath = _schema.Indexes.ToImmutableDictionary(i => i.PropertyPath);
            _databaseService = databaseService;
            QueryOp = new QueryOp<T>(_schema.Indexes
                .ToImmutableDictionary(i => i.PropertyPath, i => i));
        }

        public QueryOp<T> QueryOp { get; }

        public IImmutableList<T> Query(
            PredicateBase<T> predicate,
            TransactionContext? transactionContext = null)
        {
            var implicitContext = transactionContext ?? _databaseService.CreateTransaction();
            var transactionId = transactionContext != null
                ? transactionContext.TransactionId
                : implicitContext.TransactionId;
            var transactionCache = _databaseService.GetTransactionCache(transactionId);

            try
            {
                while (true)
                {
                    var primitivePredicate = predicate.FirstPrimitivePredicate;

                    if (primitivePredicate == null)
                    {
                        var result = QueryResult(predicate);

                        implicitContext.Complete();

                        return result;
                    }
                    else if (primitivePredicate is IIndexEqual<T> ie)
                    {
                        var revisionIds = FindEqualHash(
                            transactionCache,
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
            finally
            {
                ((IDisposable)implicitContext).Dispose();
            }
        }

        private IImmutableSet<long> FindEqualHash(
            TransactionCache transactionCache,
            string propertyPath,
            short keyHash)
        {
            var revisionIds = ImmutableHashSet<long>.Empty.ToBuilder();
            var key = new TableIndexHash(
                new TableIndexKey(_schema.TableName, propertyPath),
                keyHash);

            //  From past transaction
            foreach (var log in transactionCache.DatabaseCache.TransactionLogs)
            {   //  Remove documents that were removed in a transaction
                revisionIds.ExceptWith(log.DeletedDocuments);
                //  Match hashed index value
                if (log.NewIndexes.TryGetValue(key, out var pastRevisionIds))
                {
                    revisionIds.Union(pastRevisionIds);
                }
            }
            //  From current transaction
            //  Remove documents that were removed in a transaction
            revisionIds.ExceptWith(transactionCache.TransactionLog.DeletedDocuments);
            //  Match hashed index value
            if (transactionCache.TransactionLog.NewIndexes.TryGetValue(
                key,
                out var newRevisionIds))
            {
                revisionIds.Union(newRevisionIds);
            }

            return revisionIds.ToImmutable();
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

        public void AppendDocument(T document, TransactionContext? transactionContext = null)
        {
            var implicitContext = transactionContext ?? _databaseService.CreateTransaction();
            var transactionId = transactionContext != null
                ? transactionContext.TransactionId
                : implicitContext.TransactionId;
            var transactionCache = _databaseService.GetTransactionCache(transactionId);

            try
            {
                var revisionId = _databaseService.GetNewDocumentRevisionId();
                var serializedDocument = Serialize(document);

                transactionCache.TransactionLog.AppendDocument(revisionId, serializedDocument);
                foreach (var index in _schema.Indexes)
                {
                    transactionCache.TransactionLog.AppendIndexValue(
                        new TableIndexHash(
                            new TableIndexKey(_schema.TableName, index.PropertyPath),
                            index.HashExtractor(document)),
                        revisionId);
                }
                implicitContext.Complete();
            }
            finally
            {
                ((IDisposable)implicitContext).Dispose();
            }
        }

        public long DeleteDocuments(Expression<Func<T, bool>> predicate)
        {
            throw new NotImplementedException();
        }

        #region Serialization
        private byte[] Serialize(T document)
        {
            var bufferWriter = new ArrayBufferWriter<byte>();

            using (var writer = new Utf8JsonWriter(bufferWriter))
            {
                JsonSerializer.Serialize(writer, document);
            }

            return bufferWriter.WrittenMemory.ToArray();
        }
        #endregion
    }
}