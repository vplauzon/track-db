using Ipdb.Lib2.Cache;
using Ipdb.Lib2.Cache.CachedBlock;
using Ipdb.Lib2.Query;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Ipdb.Lib2
{
    public class TableQuery : IEnumerable<ReadOnlyMemory<object?>>
    {
        private readonly Table _table;
        private readonly TransactionContext? _transactionContext;
        private readonly IQueryPredicate _predicate;
        private readonly IImmutableList<int> _projectionColumnIndexes;
        private readonly int? _takeCount;

        #region Constructors
        internal TableQuery(
            Table table,
            TransactionContext? transactionContext,
            IQueryPredicate predicate,
            IEnumerable<int>? projectionColumnIndexes,
            int? takeCount)
        {
            _table = table;
            _transactionContext = transactionContext;
            _predicate = predicate;
            _projectionColumnIndexes =
                (projectionColumnIndexes ?? Enumerable.Range(0, _table.Schema.Columns.Count))
                .ToImmutableArray();
            _takeCount = takeCount;
        }
        #endregion

        #region IEnumerator<T>
        IEnumerator<ReadOnlyMemory<object?>> IEnumerable<ReadOnlyMemory<object?>>.GetEnumerator()
        {
            return ExecuteQuery().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ExecuteQuery().GetEnumerator();
        }
        #endregion

        public long Count()
        {
            throw new NotImplementedException();
        }

        public void Delete()
        {
            _table.Database.ExecuteWithinTransactionContext(
                _transactionContext,
                transactionCache =>
                {
                    if (_takeCount != 0)
                    {
                        transactionCache.UncommittedTransactionLog.TableTransactionLogMap.TryGetValue(
                            _table.Schema.TableName,
                            out var uncommittedLog);

                        var deletedRecordIds = ExecuteQuery(
                            transactionCache,
                            (block, result) =>
                            {
                                return result.RecordId;
                            })
                        .ToImmutableList();
                        var uncommittedBlock = uncommittedLog?.BlockBuilder;
                        var uncommittedDeletedRecordIds = uncommittedBlock
                        ?.DeleteRecords(deletedRecordIds)
                        .ToImmutableHashSet();
                        var committedDeletedRecordIds = uncommittedDeletedRecordIds == null
                        ? deletedRecordIds
                        : deletedRecordIds.Where(id => !uncommittedDeletedRecordIds.Contains(id));

                        transactionCache.UncommittedTransactionLog.DeleteRecordIds(
                            committedDeletedRecordIds,
                            _table.Schema);
                    }
                });
        }

        #region Query internals
        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQuery()
        {
            var results = _table.Database.ExecuteWithinTransactionContext(
                _transactionContext,
                transactionCache =>
                {
                    if (_takeCount == 0)
                    {
                        return Array.Empty<ReadOnlyMemory<object?>>();
                    }
                    else
                    {
                        return ExecuteQuery(
                            transactionCache,
                            (block, result) =>
                            {
                                var row = result.ProjectionFunc();

                                return new ReadOnlyMemory<object?>(row);
                            });
                    }
                });

            return results;
        }

        private IEnumerable<U> ExecuteQuery<U>(
            TransactionCache transactionCache,
            Func<IBlock, QueryResult, U> extractResultFunc)
        {
            var takeCount = _takeCount ?? int.MaxValue;
            var deletedRecordIds = transactionCache.ListDeletedRecordIds(_table.Schema.TableName)
                .ToImmutableHashSet();

            foreach (var block in ListBlocks(transactionCache))
            {
                var results = block.Query(_predicate, _projectionColumnIndexes);

                foreach (var result in RemoveDeleted(deletedRecordIds, results))
                {
                    yield return extractResultFunc(block, result);
                    --takeCount;
                    if (takeCount == 0)
                    {
                        yield break;
                    }
                }
            }
        }

        private IEnumerable<QueryResult> RemoveDeleted(
            IImmutableSet<long> deletedRecordIds,
            IEnumerable<QueryResult> results)
        {
            foreach (var result in results)
            {
                if (!deletedRecordIds.Contains(result.RecordId))
                {
                    yield return result;
                }
            }
        }

        private IEnumerable<IBlock> ListBlocks(TransactionCache transactionCache)
        {
            foreach (var block in transactionCache.ListTransactionLogBlocks(_table.Schema.TableName))
            {
                yield return block;
            }
        }
        #endregion
    }
}