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
    /// <summary>Query on a table.</summary>
    public class TableQuery : IEnumerable<ReadOnlyMemory<object?>>
    {
        #region Inner types
        private record struct InnerState(
            Table QueryTable,
            TransactionContext? Tx,
            bool CanDelete,
            bool InMemoryOnly,
            bool InTxOnly,
            bool CommittedOnly,
            QueryPredicate Predicate,
            IImmutableList<int> ProjectionColumnIndexes,
            IImmutableList<SortColumn> SortColumns,
            int? TakeCount,
            bool IgnoreDeleted,
            string? QueryTag);

        private record IdentifiedBlock(int BlockId, IBlock Block);

        private record BlockRowIndex(int BlockId, int RowIndex);

        private record SortedResult(BlockRowIndex BlockRowIndex, ReadOnlyMemory<object?>? Result);
        #endregion

        private readonly InnerState _innerState;

        #region Constructors
        internal TableQuery(
            Table table,
            TransactionContext? tx,
            bool canDelete)
            : this(
                  new InnerState(
                      table,
                      tx,
                      canDelete,
                      false,
                      false,
                      false,
                      AllInPredicate.Instance,
                      Enumerable.Range(0, table.Schema.Columns.Count).ToImmutableArray(),
                      ImmutableArray<SortColumn>.Empty,
                      null,
                      false,
                      null))
        {
        }

        private TableQuery(InnerState innerState)
        {
            _innerState = innerState;
        }
        #endregion

        #region Alterations
        public TableQuery WithPredicate(QueryPredicate predicate)
        {
            return new TableQuery(
                _innerState with
                {
                    Predicate = predicate.Simplify() ?? predicate
                });
        }

        public TableQuery WithProjection(params IEnumerable<int> projectionColumnIndexes)
        {
            return new TableQuery(
                _innerState with
                {
                    ProjectionColumnIndexes = projectionColumnIndexes.ToImmutableArray()
                });
        }

        public TableQuery WithSortColumns(IEnumerable<SortColumn> sortColumns)
        {
            return new TableQuery(
                _innerState with
                {
                    SortColumns = sortColumns.ToImmutableArray()
                });
        }

        public TableQuery WithTake(int? takeCount)
        {
            return new TableQuery(
                _innerState with
                {
                    TakeCount = takeCount
                });
        }

        public TableQuery WithinTransactionOnly()
        {
            return new TableQuery(
                _innerState with
                {
                    InTxOnly = true
                });
        }

        internal TableQuery WithIgnoreDeleted()
        {
            return new TableQuery(
                _innerState with
                {
                    IgnoreDeleted = true
                });
        }

        internal TableQuery WithQueryTag(string queryTag)
        {
            return new TableQuery(
                _innerState with
                {
                    QueryTag = queryTag
                });
        }

        internal TableQuery WithInMemoryOnly()
        {
            return new TableQuery(
                _innerState with
                {
                    InMemoryOnly = true
                });
        }

        internal TableQuery WithCommittedOnly()
        {
            return new TableQuery(
                _innerState with
                {
                    CommittedOnly = true
                });
        }
        #endregion

        #region IEnumerator<T>
        IEnumerator<ReadOnlyMemory<object?>> IEnumerable<ReadOnlyMemory<object?>>.GetEnumerator()
        {
            if (_innerState.ProjectionColumnIndexes.Any())
            {
                return ExecuteQuery(_innerState.ProjectionColumnIndexes, false).GetEnumerator();
            }
            else
            {
                throw new InvalidOperationException("No columns would be projected.");
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            if (_innerState.ProjectionColumnIndexes.Any())
            {
                return ExecuteQuery(_innerState.ProjectionColumnIndexes, false).GetEnumerator();
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
                var columnNames = _innerState.QueryTable.Schema.ColumnProperties
                    .Select(c => c.ColumnSchema.ColumnName);
                var dataTable = new DataTable();
                var query = this.WithProjection(
                    Enumerable.Range(0, _innerState.QueryTable.Schema.ColumnProperties.Count));

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
            if (!_innerState.CanDelete)
            {
                throw new UnauthorizedAccessException("Can't delete records on this table");
            }

            return _innerState.QueryTable.Database.ExecuteWithinTransactionContext(
                _innerState.Tx,
                tx =>
                {
                    if (_innerState.TakeCount != 0)
                    {
                        //  Get the transaction table log if it exists, otherwise get null
                        tx.TransactionState.UncommittedTransactionLog.TransactionTableLogMap.TryGetValue(
                            _innerState.QueryTable.Schema.TableName,
                            out var transactionTableLog);

                        //  Fetch record & block ID of records matching query
                        var deletedRecordIds = ExecuteQuery(
                            tx,
                            true,
                            [
                                _innerState.QueryTable.Schema.RecordIdColumnIndex,
                                _innerState.QueryTable.Schema.ParentBlockIdColumnIndex])
                        .Select(r => (long)r.Span[0]!)
                        .ToImmutableList();
                        //  Hard delete the records that are uncommitted ONLY
                        var hardDeletedRecordIds = transactionTableLog
                        ?.NewDataBlock
                        .DeleteRecordsByRecordId(deletedRecordIds)
                        .ToArray();

                        foreach (var recordId in deletedRecordIds)
                        {
                            if (hardDeletedRecordIds == null
                                || !hardDeletedRecordIds.Contains(recordId))
                            {
                                _innerState.QueryTable.Database.DeleteRecord(
                                    recordId,
                                    _innerState.QueryTable.Schema.TableName,
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
            var results = _innerState.QueryTable.Database.EnumeratesWithinTransactionContext(
                _innerState.Tx,
                tx =>
                {
                    if (_innerState.TakeCount == 0
                    || (_innerState.InTxOnly && _innerState.CommittedOnly))
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
            if (!noSort && _innerState.SortColumns.Any())
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
            var takeCount = _innerState.TakeCount ?? int.MaxValue;
            var isTableMeta = _innerState.QueryTable.Schema is MetadataTableSchema;
            var isTableTombstone =
                _innerState.QueryTable.Schema.TableName
                == _innerState.QueryTable.Database.TombstoneTable.Schema.TableName;
            var predicate = _innerState.IgnoreDeleted || isTableMeta || isTableTombstone
                ? _innerState.Predicate
                //  Remove deleted records
                : new ConjunctionPredicate(
                    _innerState.Predicate,
                    new InPredicate<long>(
                        _innerState.QueryTable.Schema.RecordIdColumnIndex,
                        _innerState.QueryTable.Database.GetDeletedRecordIds(
                            _innerState.QueryTable.Schema.TableName, tx),
                        false));
            var materializedProjectionColumnIndexes = projectionColumnIndexes
                .ToImmutableArray();
            var buffer = new object?[materializedProjectionColumnIndexes.Length].AsMemory();
            var queryId = Guid.NewGuid().ToString();

            predicate = predicate.Simplify() ?? predicate;
            foreach (var block in ListBlocks(tx))
            {
                var filterOutput = block.Block.Filter(predicate, _innerState.QueryTag != null);
                var results = block.Block.Project(
                    buffer,
                    materializedProjectionColumnIndexes,
                    filterOutput.RowIndexes,
                    block.BlockId);

                AuditPredicate(filterOutput.PredicateAuditTrails, block.BlockId, queryId);
                foreach (var result in results)
                {
                    yield return result;
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
            if (_innerState.QueryTag != null)
            {
                var records = predicateAuditTrails
                    .Select(p => new QueryExecutionRecord(
                        p.Timestamp,
                        _innerState.QueryTable.Schema.TableName,
                        queryId,
                        _innerState.QueryTag,
                        blockId,
                        p.Iteration,
                        p.Predicate.ToString()));

                _innerState.QueryTable.Database.QueryExecutionTable.AppendRecords(records);
            }
        }

        private IEnumerable<IdentifiedBlock> ListBlocks(TransactionContext tx)
        {
            if (_innerState.InMemoryOnly || _innerState.InTxOnly)
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
                transactionState.ListBlocks(
                    _innerState.QueryTable.Schema.TableName,
                    _innerState.InTxOnly,
                    _innerState.CommittedOnly))
            {
                yield return new IdentifiedBlock(blockId--, block);
            }
        }

        private IEnumerable<IdentifiedBlock> ListPersistedBlocks(TransactionContext tx)
        {
            if (_innerState.QueryTable.Database.HasMetaDataTable(
                _innerState.QueryTable.Schema.TableName))
            {
                var metaDataTable = _innerState.QueryTable.Database.GetMetaDataTable(
                    _innerState.QueryTable.Schema.TableName);
                var metaSchema = (MetadataTableSchema)metaDataTable.Schema;
                var metaPredicate = MetaPredicateHelper.GetCorrespondantPredicate(
                    _innerState.Predicate,
                    _innerState.QueryTable.Schema,
                    metaSchema);
                var metaDataQuery = metaDataTable.Query(tx)
                    .WithProjection([metaSchema.BlockIdColumnIndex])
                    .WithPredicate(metaPredicate);

                if (_innerState.QueryTag != null)
                {
                    metaDataQuery = metaDataQuery.WithQueryTag(_innerState.QueryTag);
                }
                foreach (var metaDataRow in metaDataQuery)
                {
                    var blockId = (int)metaDataRow.Span[0]!;

                    yield return new IdentifiedBlock(
                        blockId,
                        _innerState.QueryTable.Database.GetOrLoadBlock(
                            blockId,
                            _innerState.QueryTable.Schema));
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
                return _innerState.QueryTable.Database.GetOrLoadBlock(blockId, _innerState.QueryTable.Schema);
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
                .Append(_innerState.QueryTable.Schema.RecordIndexColumnIndex)
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

        private IEnumerable<BlockRowIndex> SortAndTruncateSortColumns(TransactionContext tx)
        {
            var takeCount = _innerState.TakeCount ?? int.MaxValue;
            var isTableMeta = _innerState.QueryTable.Schema is MetadataTableSchema;
            var predicate = !_innerState.IgnoreDeleted && !isTableMeta
                ? new ConjunctionPredicate(
                    _innerState.Predicate,
                    new InPredicate<long>(
                        _innerState.QueryTable.Schema.RecordIdColumnIndex,
                        _innerState.QueryTable.Database.GetDeletedRecordIds(
                            _innerState.QueryTable.Schema.TableName, tx),
                        false))
                : _innerState.Predicate;
            var projectionColumnIndexes = _innerState.SortColumns
                .Select(s => s.ColumnIndex)
                .Append(_innerState.QueryTable.Schema.RecordIndexColumnIndex)
                .Append(_innerState.QueryTable.Schema.ParentBlockIdColumnIndex)
                .Append(_innerState.QueryTable.Schema.RecordIdColumnIndex)
                .ToImmutableArray();
            var buffer = new object?[projectionColumnIndexes.Length].AsMemory();
            var accumulatedSortValues = ImmutableArray<object?[]>.Empty;
            var areSortValuesSorted = false;
            var queryId = Guid.NewGuid().ToString();

            foreach (var block in ListBlocks(tx))
            {
                var filterOutput = block.Block.Filter(predicate, _innerState.QueryTag != null);
                var results = block.Block.Project(
                    buffer,
                    projectionColumnIndexes,
                    filterOutput.RowIndexes,
                    block.BlockId);
                var newSortValues = results
                    .Select(r => r.ToArray())
                    .ToArray();

                AuditPredicate(filterOutput.PredicateAuditTrails, block.BlockId, queryId);
                if (accumulatedSortValues.Length + newSortValues.Length > _innerState.TakeCount)
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

            for (var i = 0; i != _innerState.SortColumns.Count; ++i)
            {   //  Materialize value 'i' in the for loop
                var j = i;

                if (sortedSortValues == null)
                {
                    if (_innerState.SortColumns[i].IsAscending)
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
                    if (_innerState.SortColumns[i].IsAscending)
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