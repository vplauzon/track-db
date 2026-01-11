using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
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
        private readonly TransactionContext? _tx;
        private readonly bool _canDelete;
        private readonly bool _inMemoryOnly;
        private readonly bool _inTxOnly;
        private readonly bool _committedOnly;
        private readonly QueryPredicate _predicate;
        private readonly IImmutableList<int> _projectionColumnIndexes;
        private readonly IImmutableList<SortColumn> _sortColumns;
        private readonly int? _takeCount;
        private readonly bool _ignoreDeleted;
        private readonly string? _queryTag;

        #region Constructors
        internal TableQuery(
            Table table,
            TransactionContext? tx,
            bool canDelete)
            : this(
                  table,
                  tx,
                  canDelete,
                  false,
                  false,
                  false,
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
            TransactionContext? tx,
            bool canDelete,
            bool inMemoryOnly,
            bool inTxOnly,
            bool notInTransactionOnly,
            QueryPredicate predicate,
            IEnumerable<int> projectionColumnIndexes,
            IEnumerable<SortColumn> sortColumns,
            int? takeCount,
            bool ignoreDeleted,
            string? queryTag)
        {
            _table = table;
            _tx = tx;
            _canDelete = canDelete;
            _inMemoryOnly = inMemoryOnly;
            _inTxOnly = inTxOnly;
            _committedOnly = notInTransactionOnly;
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
                _tx,
                _canDelete,
                _inMemoryOnly,
                _inTxOnly,
                _committedOnly,
                predicate,
                _projectionColumnIndexes,
                _sortColumns,
                _takeCount,
                _ignoreDeleted,
                _queryTag);
        }

        public TableQuery WithProjection(params IEnumerable<int> projectionColumnIndexes)
        {
            return new TableQuery(
                _table,
                _tx,
                _canDelete,
                _inMemoryOnly,
                _inTxOnly,
                _committedOnly,
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
                _tx,
                _canDelete,
                _inMemoryOnly,
                _inTxOnly,
                _committedOnly,
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
                _tx,
                _canDelete,
                _inMemoryOnly,
                _inTxOnly,
                _committedOnly,
                _predicate,
                _projectionColumnIndexes,
                _sortColumns,
                takeCount,
                _ignoreDeleted,
                _queryTag);
        }

        public TableQuery WithinTransactionOnly()
        {
            return new TableQuery(
                _table,
                _tx,
                _canDelete,
                _inMemoryOnly,
                true,
                _committedOnly,
                _predicate,
                _projectionColumnIndexes,
                _sortColumns,
                _takeCount,
                _ignoreDeleted,
                _queryTag);
        }

        internal TableQuery WithIgnoreDeleted()
        {
            return new TableQuery(
                _table,
                _tx,
                _canDelete,
                _inMemoryOnly,
                _inTxOnly,
                _committedOnly,
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
                _tx,
                _canDelete,
                _inMemoryOnly,
                _inTxOnly,
                _committedOnly,
                _predicate,
                _projectionColumnIndexes,
                _sortColumns,
                _takeCount,
                true,
                queryTag);
        }

        internal TableQuery WithInMemoryOnly()
        {
            return new TableQuery(
                _table,
                _tx,
                _canDelete,
                true,
                _inTxOnly,
                _committedOnly,
                _predicate,
                _projectionColumnIndexes,
                _sortColumns,
                _takeCount,
                true,
                _queryTag);
        }

        internal TableQuery WithCommittedOnly()
        {
            return new TableQuery(
                _table,
                _tx,
                _canDelete,
                _inMemoryOnly,
                _inTxOnly,
                true,
                _predicate,
                _projectionColumnIndexes,
                _sortColumns,
                _takeCount,
                true,
                _queryTag);
        }
        #endregion

        #region IEnumerator<T>
        IEnumerator<ReadOnlyMemory<object?>> IEnumerable<ReadOnlyMemory<object?>>.GetEnumerator()
        {
            if (_projectionColumnIndexes.Any())
            {
                return ExecuteQuery(_projectionColumnIndexes, false).GetEnumerator();
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
                return ExecuteQuery(_projectionColumnIndexes, false).GetEnumerator();
            }
            else
            {
                throw new InvalidOperationException("No columns would be projected.");
            }
        }
        #endregion

        #region Debug View
        /// <summary>To be used in debugging only.</summary>
        internal DataTable DebugView
        {
            get
            {
                var columnNames = _table.Schema.ColumnProperties
                    .Select(c => c.ColumnSchema.ColumnName);
                var dataTable = new DataTable();
                var query = this.WithProjection(
                    Enumerable.Range(0, _table.Schema.ColumnProperties.Count));

                foreach (var columnName in columnNames)
                {
                    dataTable.Columns.Add(columnName);
                }
                foreach (var record in query)
                {
                    dataTable.Rows.Add(record.ToArray());
                }

                return dataTable;
            }
        }
        #endregion

        public bool Any()
        {
            //  Project no columns & sort no columns
            var items = ExecuteQuery(Array.Empty<int>(), false);

            return items.Any();
        }

        public long Count()
        {
            //  Project no columns & sort no columns
            var items = ExecuteQuery(Array.Empty<int>(), false);
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
                _tx,
                tx =>
                {
                    if (_takeCount != 0)
                    {
                        //  Get the transaction table log if it exists, otherwise get null
                        tx.TransactionState.UncommittedTransactionLog.TransactionTableLogMap.TryGetValue(
                            _table.Schema.TableName,
                            out var transactionTableLog);

                        //  Fetch record & block ID of records matching query
                        var deletedRecordIds = ExecuteQuery(
                            tx,
                            true,
                            [_table.Schema.RecordIdColumnIndex, _table.Schema.ParentBlockIdColumnIndex])
                        .Select(r => new
                        {
                            RecordId = (long)r.Span[0]!,
                            BlockId = (int)r.Span[1]!
                        })
                        .ToImmutableList();
                        //  Hard delete the records that are uncommitted ONLY
                        var hardDeletedRecordIds = transactionTableLog
                        ?.NewDataBlock
                        .DeleteRecordsByRecordId(deletedRecordIds.Select(r => r.RecordId))
                        .ToArray();

                        foreach (var r in deletedRecordIds)
                        {
                            if (hardDeletedRecordIds == null
                                || !hardDeletedRecordIds.Contains(r.RecordId))
                            {
                                _table.Database.DeleteRecord(
                                    r.RecordId,
                                    r.BlockId <= 0 ? null : r.BlockId,
                                    _table.Schema.TableName,
                                    tx);
                            }
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
            bool noSort)
        {
            var results = _table.Database.EnumeratesWithinTransactionContext(
                _tx,
                tx =>
                {
                    if (_takeCount == 0 || (_inTxOnly && _committedOnly))
                    {
                        return Array.Empty<ReadOnlyMemory<object?>>();
                    }
                    else
                    {
                        return ExecuteQuery(tx, noSort, projectionColumnIndexes);
                    }
                });

            return results;
        }

        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQuery(
            TransactionContext tx,
            bool noSort,
            IEnumerable<int> projectionColumnIndexes)
        {
            if (!noSort && _sortColumns.Any())
            {
                return ExecuteQueryWithSort(
                    tx,
                    projectionColumnIndexes);
            }
            else
            {
                return ExecuteQueryWithoutSort(
                    tx,
                    projectionColumnIndexes);
            }
        }

        #region Query without sort
        private IEnumerable<ReadOnlyMemory<object?>> ExecuteQueryWithoutSort(
            TransactionContext tx,
            IEnumerable<int> projectionColumnIndexes)
        {
            var takeCount = _takeCount ?? int.MaxValue;
            var deletedRecordIds = !_ignoreDeleted
                ? _table.Database.GetDeletedRecordIds(
                    _table.Schema.TableName,
                    tx)
                .ToImmutableHashSet()
                : ImmutableHashSet<long>.Empty;
            var materializedProjectionColumnIndexes = projectionColumnIndexes
                //  Add Record ID at the end, so we can use it to detect deleted rows
                .Append(_table.Schema.RecordIdColumnIndex)
                .ToImmutableArray();
            var buffer = new object?[materializedProjectionColumnIndexes.Length].AsMemory();
            var queryId = Guid.NewGuid().ToString();

            foreach (var block in ListBlocks(tx))
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
                var recordId = (long)result.Span[result.Length - 1]!;

                if (!deletedRecordIds.Contains(recordId))
                {
                    yield return result;
                }
            }
        }

        private IEnumerable<IdentifiedBlock> ListBlocks(TransactionContext tx)
        {
            if (_inMemoryOnly || _inTxOnly)
            {
                return ListUnpersistedBlocks(tx);
            }
            else
            {
                return ListUnpersistedBlocks(tx)
                    .Concat(ListPersistedBlocks(tx));
            }
        }

        private IEnumerable<IdentifiedBlock> ListUnpersistedBlocks(TransactionContext transactionContext)
        {
            var transactionState = transactionContext.TransactionState;
            var blockId = 0;

            foreach (var block in
                transactionState.ListBlocks(_table.Schema.TableName, _inTxOnly, _committedOnly))
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
            TransactionContext tx,
            IEnumerable<int> projectionColumnIndexes)
        {
            //  First phase sort + truncate sort columns
            var sortedResults = SortAndTruncateSortColumns(tx)
                .Select(i => new SortedResult(i, null))
                .ToImmutableArray();
            //  Second phase:  re-query blocks to project results
            var materializedProjectionColumnIndexes = projectionColumnIndexes
                .Append(_table.Schema.RecordIndexColumnIndex)
                .ToImmutableArray();
            var buffer = new object?[materializedProjectionColumnIndexes.Length].AsMemory();

            while (sortedResults.Any())
            {
                var firstBlockId = sortedResults.First().BlockRowIndex.BlockId;
                var rowIndexes = sortedResults
                    .Where(r => r.BlockRowIndex.BlockId == firstBlockId)
                    .Select(r => r.BlockRowIndex.RowIndex);
                var block = GetBlock(tx, firstBlockId);
                var resultMap = block.Project(
                    buffer,
                    materializedProjectionColumnIndexes,
                    rowIndexes,
                    firstBlockId)
                    .ToImmutableDictionary(
                    r => ((int)(r.Span[materializedProjectionColumnIndexes.Length - 1])!),
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
                .Append(_table.Schema.RecordIndexColumnIndex)
                .Append(_table.Schema.ParentBlockIdColumnIndex)
                .Append(_table.Schema.RecordIdColumnIndex)
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