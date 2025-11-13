using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    /// <summary>
    /// Query on a table.  The first N columns are the table's schema.
    /// Column N is the record ID (long).
    /// Column N+1 is the row index within the block.
    /// Column N+2 is the block ID.
    /// </summary>
    public class TableQuery : IEnumerable<ReadOnlyMemory<object?>>
    {
        #region Inner types
        private record IdentifiedBlock(int BlockId, IBlock Block);

        private record BlockRowIndex(int BlockId, int RowIndex);

        private record SortedResult(BlockRowIndex BlockRowIndex, ReadOnlyMemory<object?>? Result);
        #endregion

        private readonly Table _table;
        private readonly bool _canDelete;
        private readonly TransactionContext? _transactionContext;
        private readonly QueryPredicate _predicate;
        private readonly IImmutableList<int> _projectionColumnIndexes;
        private readonly IImmutableList<SortColumn> _sortColumns;
        private readonly int? _takeCount;
        private readonly bool _ignoreDeleted;
        private readonly string? _queryTag;

        #region Constructors
        internal TableQuery(
            Table table,
            bool canDelete,
            TransactionContext? tc)
            : this(
                  table,
                  canDelete,
                  tc,
                  AllInPredicate.Instance,
                  Enumerable.Range(0, table.Schema.Columns.Count),
                  Array.Empty<SortColumn>(),
                  null,
                  false,
                  null)
        {
        }

        private TableQuery(
            Table table,
            bool canDelete,
            TransactionContext? tc,
            QueryPredicate predicate,
            IEnumerable<int> projectionColumnIndexes,
            IEnumerable<SortColumn> sortColumns,
            int? takeCount,
            bool ignoreDeleted,
            string? queryTag)
        {
            _table = table;
            _canDelete = canDelete;
            _transactionContext = tc;
            _predicate = predicate.Simplify() ?? predicate;
            _projectionColumnIndexes = projectionColumnIndexes.ToImmutableArray();
            _sortColumns = sortColumns.ToImmutableArray();
            _takeCount = takeCount;
            _ignoreDeleted = ignoreDeleted;
            _queryTag = queryTag;
        }
        #endregion

        #region Alterations
        public TableQuery WithPredicate(QueryPredicate predicate)
        {
            return new TableQuery(
                _table,
                _canDelete,
                _transactionContext,
                predicate,
                _projectionColumnIndexes,
                _sortColumns,
                _takeCount,
                _ignoreDeleted,
                _queryTag);
        }

        public TableQuery WithProjection(IEnumerable<int> projectionColumnIndexes)
        {
            return new TableQuery(
                _table,
                _canDelete,
                _transactionContext,
                _predicate,
                projectionColumnIndexes.ToImmutableArray(),
                _sortColumns,
                _takeCount,
                _ignoreDeleted,
                _queryTag);
        }

        public TableQuery WithSortColumns(IEnumerable<SortColumn> sortColumns)
        {
            return new TableQuery(
                _table,
                _canDelete,
                _transactionContext,
                _predicate,
                _projectionColumnIndexes,
                sortColumns,
                _takeCount,
                _ignoreDeleted,
                _queryTag);
        }

        public TableQuery WithTake(int? takeCount)
        {
            return new TableQuery(
                _table,
                _canDelete,
                _transactionContext,
                _predicate,
                _projectionColumnIndexes,
                _sortColumns,
                takeCount,
                _ignoreDeleted,
                _queryTag);
        }

        internal TableQuery WithIgnoreDeleted()
        {
            return new TableQuery(
                _table,
                _canDelete,
                _transactionContext,
                _predicate,
                _projectionColumnIndexes,
                _sortColumns,
                _takeCount,
                true,
                _queryTag);
        }

        internal TableQuery WithQueryTag(string queryTag)
        {
            return new TableQuery(
                _table,
                _canDelete,
                _transactionContext,
                _predicate,
                _projectionColumnIndexes,
                _sortColumns,
                _takeCount,
                true,
                queryTag);
        }
        #endregion

        #region IEnumerator<T>
        IEnumerator<ReadOnlyMemory<object?>> IEnumerable<ReadOnlyMemory<object?>>.GetEnumerator()
        {
            if (_projectionColumnIndexes.Any())
            {
                return ExecuteQuery(_projectionColumnIndexes, _sortColumns).GetEnumerator();
            }
            else
            {
                throw new InvalidOperationException("No columns would be projected.");
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            if (_projectionColumnIndexes.Any())
            {
                return ExecuteQuery(_projectionColumnIndexes, _sortColumns).GetEnumerator();
            }
            else
            {
                throw new InvalidOperationException("No columns would be projected.");
            }
        }
        #endregion

        public long Count()
        {
            //  Project no columns & sort no columns
            var items = ExecuteQuery(Array.Empty<int>(), Array.Empty<SortColumn>());
            var count = (long)0;

            foreach (var item in items)
            {
                ++count;
            }

            return count;
        }

        /// <summary>
        /// Delete record matching query predicate.
        /// </summary>
        /// <returns>Number of records deleted.</returns>
        public int Delete()
        {
            if (!_canDelete)
            {
                throw new UnauthorizedAccessException("Can't delete records on this table");
            }

            return _table.Database.ExecuteWithinTransactionContext(
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
                            //  Fetch record ID & block ID
                            [_table.Schema.Columns.Count, _table.Schema.Columns.Count + 2])
                        .Select(r => new
                        {
                            RecordId = (long)r.Span[0]!,
                            BlockId = (int)r.Span[1]!
                        })
                        .ToImmutableList();
                        var uncommittedDeletedRecordIds = blockBuilder
                        ?.DeleteRecordsByRecordId(deletedRecordIds.Select(r => r.RecordId))
                        .ToImmutableHashSet();
                        var committedDeletedRecordIds = uncommittedDeletedRecordIds == null
                        ? deletedRecordIds
                        : deletedRecordIds.Where(r => !uncommittedDeletedRecordIds.Contains(r.RecordId));

                        foreach (var r in committedDeletedRecordIds)
                        {
                            _table.Database.DeleteRecord(
                                r.RecordId,
                                r.BlockId <= 0 ? null : r.BlockId,
                                _table.Schema.TableName,
                                tc);
                        }

                        return deletedRecordIds.Count;
                    }
                    else
                    {
                        return 0;
                    }
                });
        }

        #region Query internals
        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQuery(
            IEnumerable<int> projectionColumnIndexes,
            IEnumerable<SortColumn> sortColumns)
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
                        return ExecuteQuery(tc, projectionColumnIndexes);
                    }
                });

            return results;
        }

        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQuery(
            TransactionContext transactionContext,
            IEnumerable<int> projectionColumnIndexes)
        {
            if (_sortColumns.Any())
            {
                return ExecuteQueryWithSort(
                    transactionContext,
                    projectionColumnIndexes);
            }
            else
            {
                return ExecuteQueryWithoutSort(
                    transactionContext,
                    projectionColumnIndexes);
            }
        }

        #region Query without sort
        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQueryWithoutSort(
            TransactionContext transactionContext,
            IEnumerable<int> projectionColumnIndexes)
        {
            var takeCount = _takeCount ?? int.MaxValue;
            var deletedRecordIds = !_ignoreDeleted
                ? _table.Database.GetDeletedRecordIds(
                    _table.Schema.TableName,
                    transactionContext)
                .ToImmutableHashSet()
                : ImmutableHashSet<long>.Empty;
            var materializedProjectionColumnIndexes = projectionColumnIndexes
                //  Add Record ID at the end, so we can use it to detect deleted rows
                .Append(_table.Schema.RecordIdColumnIndex)
                .ToImmutableArray();
            var buffer = new object?[materializedProjectionColumnIndexes.Length].AsMemory();
            var queryId = Guid.NewGuid().ToString();

            foreach (var block in ListBlocks(transactionContext))
            {
                var filterOutput = block.Block.Filter(_predicate, _queryTag != null);
                var results = block.Block.Project(
                    buffer,
                    materializedProjectionColumnIndexes,
                    filterOutput.RowIndexes,
                    block.BlockId);

                AuditPredicate(filterOutput.PredicateAuditTrails, block.BlockId, queryId);
                foreach (var result in RemoveDeleted(deletedRecordIds, results))
                {
                    //  Remove last column (record ID)
                    yield return result.Slice(0, result.Length - 1);
                    --takeCount;
                    if (takeCount == 0)
                    {
                        yield break;
                    }
                }
            }
        }

        private void AuditPredicate(
            IEnumerable<PredicateAuditTrail> predicateAuditTrails,
            int blockId,
            string queryId)
        {
            if (_queryTag != null)
            {
                var records = predicateAuditTrails
                    .Select(p => new QueryExecutionRecord(
                        p.Timestamp,
                        _table.Schema.TableName,
                        queryId,
                        _queryTag,
                        blockId,
                        p.Predicate.ToString()));

                _table.Database.QueryExecutionTable.AppendRecords(records);
            }
        }

        private IEnumerable<ReadOnlyMemory<object?>> RemoveDeleted(
            IImmutableSet<long> deletedRecordIds,
            IEnumerable<ReadOnlyMemory<object?>> results)
        {
            foreach (var result in results)
            {
                var recordId = (long)result.Span[_table.Schema.RecordIdColumnIndex]!;

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

        private IEnumerable<IdentifiedBlock> ListPersistedBlocks(TransactionContext tc)
        {
            if (_table.Database.HasMetaDataTable(_table.Schema.TableName))
            {
                var metaDataTable = _table.Database.GetMetaDataTable(_table.Schema.TableName);
                var metaSchema = (MetadataTableSchema)metaDataTable.Schema;
                var metaDataQuery = metaDataTable.Query(tc)
                    .WithProjection([metaSchema.BlockIdColumnIndex])
                    //  Must be optimize to filter only blocks with relevant data
                    .WithPredicate(AllInPredicate.Instance);

                foreach (var metaDataRow in metaDataQuery)
                {
                    var blockId = (int)metaDataRow.Span[0]!;

                    yield return new IdentifiedBlock(
                        blockId,
                        _table.Database.GetOrLoadBlock(blockId, _table.Schema));
                }
            }
        }

        private IBlock GetBlock(TransactionContext tc, int blockId)
        {
            if (blockId <= 0)
            {
                return ListUnpersistedBlocks(tc)
                    .Where(i => i.BlockId == blockId)
                    .Select(i => i.Block)
                    .First();
            }
            else
            {
                return _table.Database.GetOrLoadBlock(blockId, _table.Schema);
            }
        }
        #endregion

        #region Query with sort
        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQueryWithSort(
            TransactionContext transactionContext,
            IEnumerable<int> projectionColumnIndexes)
        {
            //  First phase sort + truncate sort columns
            var sortedResults = SortAndTruncateSortColumns(transactionContext)
                .Select(i => new SortedResult(i, null))
                .ToImmutableArray();
            //  Second phase:  re-query blocks to project results
            var materializedProjectionColumnIndexes = projectionColumnIndexes
                .Append(_table.Schema.Columns.Count + 1)
                .ToImmutableArray();
            var buffer = new object?[materializedProjectionColumnIndexes.Length].AsMemory();

            while (sortedResults.Any())
            {
                var firstBlockId = sortedResults.First().BlockRowIndex.BlockId;
                var rowIndexes = sortedResults
                    .Where(r => r.BlockRowIndex.BlockId == firstBlockId)
                    .Select(r => r.BlockRowIndex.RowIndex);
                var block = GetBlock(transactionContext, firstBlockId);
                var resultMap = block.Project(
                    buffer,
                    materializedProjectionColumnIndexes,
                    rowIndexes,
                    firstBlockId)
                    .ToImmutableDictionary(
                    r => ((int?)r.Span[materializedProjectionColumnIndexes.Length - 1])!.Value,
                    r => r.Slice(0, materializedProjectionColumnIndexes.Length - 1).ToArray());
                //  Resolve result and store them in reverse order for optimal deletion
                var newSortedResults = sortedResults
                    .Select(r => r.BlockRowIndex.BlockId == firstBlockId
                    ? new SortedResult(r.BlockRowIndex, resultMap[r.BlockRowIndex.RowIndex])
                    : r)
                    .Reverse()
                    .ToList();

                //  Return available results
                while (newSortedResults.Any() && newSortedResults.Last().Result != null)
                {
                    yield return newSortedResults.Last().Result!.Value;
                    newSortedResults.RemoveAt(newSortedResults.Count() - 1);
                }
                //  Put the sequence in correct order
                sortedResults = newSortedResults.AsEnumerable().Reverse().ToImmutableArray();
            }
        }

        private IEnumerable<BlockRowIndex> SortAndTruncateSortColumns(
            TransactionContext transactionContext)
        {
            var takeCount = _takeCount ?? int.MaxValue;
            var deletedRecordIds = !_ignoreDeleted
                ? _table.Database.GetDeletedRecordIds(
                    _table.Schema.TableName,
                    transactionContext)
                .ToImmutableHashSet()
                : ImmutableHashSet<long>.Empty;
            var projectionColumnIndexes = _sortColumns
                .Select(s => s.ColumnIndex)
                //  Row index
                .Append(_table.Schema.Columns.Count + 1)
                //  Block ID
                .Append(_table.Schema.Columns.Count + 2)
                //  Record ID at the end
                .Append(_table.Schema.Columns.Count)
                .ToImmutableArray();
            var buffer = new object?[projectionColumnIndexes.Length].AsMemory();
            var accumulatedSortValues = ImmutableArray<object?[]>.Empty;
            var areSortValuesSorted = false;
            var queryId = Guid.NewGuid().ToString();

            foreach (var block in ListBlocks(transactionContext))
            {
                var filterOutput = block.Block.Filter(_predicate, _queryTag != null);
                var results = block.Block.Project(
                    buffer,
                    projectionColumnIndexes,
                    filterOutput.RowIndexes,
                    block.BlockId);
                var newSortValues = RemoveDeleted(deletedRecordIds, results)
                    .Select(r => r.ToArray())
                    .ToImmutableArray();

                AuditPredicate(filterOutput.PredicateAuditTrails, block.BlockId, queryId);
                if (accumulatedSortValues.Length + newSortValues.Length > _takeCount)
                {
                    var allSortValues = accumulatedSortValues.Concat(newSortValues);

                    accumulatedSortValues = OrderSortValues(allSortValues)
                        .Take(takeCount)
                        .ToImmutableArray();
                    areSortValuesSorted = true;
                }
                else
                {
                    accumulatedSortValues = accumulatedSortValues
                        .Concat(newSortValues)
                        .ToImmutableArray();
                }
            }
            if (!areSortValuesSorted)
            {
                accumulatedSortValues = OrderSortValues(accumulatedSortValues)
                    .Take(takeCount)
                    .ToImmutableArray();
            }

            return accumulatedSortValues
                .Select(a => a.TakeLast(3).Take(2))
                .Select(a => new BlockRowIndex(
                    ((int?)a.Last()!).Value,
                    ((int?)a.First()!).Value));
        }

        private IEnumerable<object?[]> OrderSortValues(IEnumerable<object?[]> sortValues)
        {
            IOrderedEnumerable<object?[]>? sortedSortValues = null;

            for (var i = 0; i != _sortColumns.Count; ++i)
            {   //  Materialize value 'i' in the for loop
                var j = i;

                if (sortedSortValues == null)
                {
                    if (_sortColumns[i].IsAscending)
                    {
                        sortedSortValues = sortValues.OrderBy(v => v[j]);
                    }
                    else
                    {
                        sortedSortValues = sortValues.OrderByDescending(v => v[j]);
                    }
                }
                else
                {
                    if (_sortColumns[i].IsAscending)
                    {
                        sortedSortValues = sortedSortValues.ThenBy(v => v[j]);
                    }
                    else
                    {
                        sortedSortValues = sortedSortValues.ThenByDescending(v => v[j]);
                    }
                }
            }

            return sortedSortValues!;
        }
        #endregion
        #endregion
    }
}