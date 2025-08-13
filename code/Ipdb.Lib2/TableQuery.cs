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
                tc =>
                {
                    if (_takeCount != 0)
                    {
                        tc.TransactionState.UncommittedTransactionLog.TableBlockBuilderMap.TryGetValue(
                            _table.Schema.TableName,
                            out var blockBuilder);

                        var deletedRecordIds = ExecuteQuery(
                            tc,
                            //  Fetch only the record ID
                            [_table.Schema.Columns.Count],
                            (block, result) =>
                            {
                                return (long)result.Span[0]!;
                            })
                        .ToImmutableList();
                        var uncommittedDeletedRecordIds = blockBuilder
                        ?.DeleteRecords(deletedRecordIds)
                        .ToImmutableHashSet();
                        var committedDeletedRecordIds = uncommittedDeletedRecordIds == null
                        ? deletedRecordIds
                        : deletedRecordIds.Where(id => !uncommittedDeletedRecordIds.Contains(id));

                        _table.Database.DeleteRecords(
                            committedDeletedRecordIds,
                            null,
                            _table.Schema.TableName,
                            tc);
                    }
                });
        }

        #region Query internals
        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQuery()
        {
            var results = _table.Database.EnumeratesWithinTransactionContext(
                _transactionContext,
                tc =>
                {
                    if (_takeCount == 0)
                    {
                        return Array.Empty<ReadOnlyMemory<object?>>();
                    }
                    else
                    {
                        return ExecuteQuery(
                            tc,
                            _projectionColumnIndexes,
                            (block, result) =>
                            {
                                return result;
                            });
                    }
                });

            return results;
        }

        private IEnumerable<U> ExecuteQuery<U>(
            TransactionContext transactionContext,
            IEnumerable<int> projectionColumnIndexes,
            Func<IBlock, ReadOnlyMemory<object?>, U> extractResultFunc)
        {
            var transactionCache = transactionContext.TransactionState;
            var takeCount = _takeCount ?? int.MaxValue;
            var deletedRecordIds = _table.Database.GetDeletedRecordIds(
                _table.Schema.TableName,
                transactionContext)
                .ToImmutableHashSet();

            foreach (var block in ListBlocks(transactionCache))
            {
                var results = block.Query(
                    _predicate,
                    //  Add Record ID at the end, so we can use it to detect deleted rows
                    projectionColumnIndexes.Append(_table.Schema.Columns.Count));

                foreach (var result in RemoveDeleted(deletedRecordIds, results))
                {
                    //  Remove last column (record ID)
                    yield return extractResultFunc(block, result.Slice(0, result.Length - 1));
                    --takeCount;
                    if (takeCount == 0)
                    {
                        yield break;
                    }
                }
            }
        }

        private IEnumerable<ReadOnlyMemory<object?>> RemoveDeleted(
            IImmutableSet<long> deletedRecordIds,
            IEnumerable<ReadOnlyMemory<object?>> results)
        {
            foreach (var result in results)
            {
                var recordId = (long)result.Span[result.Length - 1]!;

                if (!deletedRecordIds.Contains(recordId))
                {
                    yield return result;
                }
            }
        }

        private IEnumerable<IBlock> ListBlocks(TransactionState transactionCache)
        {
            foreach (var block in transactionCache.ListTransactionLogBlocks(_table.Schema.TableName))
            {
                yield return block;
            }
        }
        #endregion
    }
}