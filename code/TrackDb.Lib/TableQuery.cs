using TrackDb.Lib.Cache.CachedBlock;
using TrackDb.Lib.Predicate;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace TrackDb.Lib
{
    /// <summary>
    /// Query on a table.  The first N columns are the table's schema.
    /// Column N+1 is the record ID (long).
    /// Column N+2 is the row index within the block.
    /// Column N+3 is the block ID.
    /// </summary>
    public class TableQuery : IEnumerable<ReadOnlyMemory<object?>>
    {
        #region Inner types
        private record IdentifiedBlock(int BlockId, IBlock Block);
        #endregion

        private readonly Table _table;
        private readonly TransactionContext? _transactionContext;
        private readonly IQueryPredicate _predicate;
        private readonly IImmutableList<int> _projectionColumnIndexes;
        private readonly IImmutableList<int> _sortColumns;
        private readonly int? _takeCount;

        #region Constructors
        internal TableQuery(
            Table table,
            TransactionContext? transactionContext,
            IQueryPredicate predicate,
            IEnumerable<int> projectionColumnIndexes,
            IEnumerable<int> sortColumns,
            int? takeCount)
        {
            _table = table;
            _transactionContext = transactionContext;
            _predicate = predicate.Simplify() ?? predicate;
            _projectionColumnIndexes = projectionColumnIndexes.ToImmutableArray();
            _sortColumns = sortColumns.ToImmutableArray();
            _takeCount = takeCount;
        }
        #endregion

        #region IEnumerator<T>
        IEnumerator<ReadOnlyMemory<object?>> IEnumerable<ReadOnlyMemory<object?>>.GetEnumerator()
        {
            return ExecuteQuery(_projectionColumnIndexes).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ExecuteQuery(_projectionColumnIndexes).GetEnumerator();
        }
        #endregion

        public long Count()
        {
            //  Project no columns
            var items = ExecuteQuery(Array.Empty<int>());
            var count = (long)0;

            foreach (var item in items)
            {
                ++count;
            }

            return count;
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
                        ?.DeleteRecordsByRecordId(deletedRecordIds)
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
        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQuery(
            IEnumerable<int> projectionColumnIndexes)
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
                            projectionColumnIndexes,
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
            var takeCount = _takeCount ?? int.MaxValue;
            var deletedRecordIds = _table.Database.GetDeletedRecordIds(
                _table.Schema.TableName,
                transactionContext)
                .ToImmutableHashSet();
            var materializedProjectionColumnIndexes = projectionColumnIndexes
                //  Add Record ID at the end, so we can use it to detect deleted rows
                .Append(_table.Schema.Columns.Count)
                .ToImmutableArray();
            var buffer = new object?[materializedProjectionColumnIndexes.Length].AsMemory();

            foreach (var block in ListBlocks(transactionContext))
            {
                var rowIndexes = block.Block.Filter(_predicate);
                var results = block.Block.Project(
                    buffer,
                    materializedProjectionColumnIndexes,
                    rowIndexes,
                    block.BlockId);

                foreach (var result in RemoveDeleted(deletedRecordIds, results))
                {
                    //  Remove last column (record ID)
                    yield return extractResultFunc(block.Block, result.Slice(0, result.Length - 1));
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

        private IEnumerable<IdentifiedBlock> ListBlocks(TransactionContext transactionContext)
        {
            return ListUnpersistedBlocks(transactionContext)
                .Concat(ListPersistedBlocks(transactionContext));
        }

        private IEnumerable<IdentifiedBlock> ListUnpersistedBlocks(TransactionContext transactionContext)
        {
            var transactionState = transactionContext.TransactionState;
            var blockId = 0;

            foreach (var block in
                transactionState.ListTransactionLogBlocks(_table.Schema.TableName))
            {
                yield return new IdentifiedBlock(blockId--, block);
            }
        }

        private IEnumerable<IdentifiedBlock> ListPersistedBlocks(TransactionContext transactionContext)
        {
            if (_table.Database.IsMetaDataTable(_table.Schema.TableName))
            {
                var metaDataTable = _table.Database.GetMetaDataTable(_table.Schema);
                var metaDataQuery = new TableQuery(
                    metaDataTable,
                    transactionContext,
                    //  Must be optimize to filter only blocks with relevant data
                    AllInPredicate.Instance,
                    Enumerable.Range(0, metaDataTable.Schema.Columns.Count),
                    Array.Empty<int>(),
                    null);

                foreach (var metaDataRow in metaDataQuery)
                {
                    var serializedBlockMetaData = SerializedBlockMetaData.FromMetaDataRecord(
                        metaDataRow,
                        out var blockId);

                    yield return new IdentifiedBlock(
                        blockId,
                        _table.Database.GetOrLoadBlock(
                            blockId,
                            _table.Schema,
                            serializedBlockMetaData));
                }
            }
        }
        #endregion
    }
}