using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Runtime.InteropServices;
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

        private record BlockRowIndex(IReadOnlyList<BlockTrace> BlockTraces, int RowIndex);

        private record SortedResult(BlockRowIndex BlockRowIndex, ReadOnlyMemory<object?> Result);

        private class SortComparer : IComparer<SortedResult>
        {
            private readonly ImmutableArray<bool> _isAscendings;

            public SortComparer(IEnumerable<bool> isAscendings)
            {
                _isAscendings = isAscendings.ToImmutableArray();
                if (_isAscendings.Length == 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(isAscendings));
                }
            }

            int IComparer<SortedResult>.Compare(SortedResult? x, SortedResult? y)
            {
                if (x == null)
                {
                    throw new ArgumentNullException(nameof(x));
                }
                if (y == null)
                {
                    throw new ArgumentNullException(nameof(y));
                }

                var resultX = x.Result.Span;
                var resultY = y.Result.Span;

                for (var i = 0; i != _isAscendings.Length; ++i)
                {
                    var comparison = Comparer.Default.Compare(resultX[i], resultY[i]);

                    if (comparison != 0)
                    {
                        return _isAscendings[i] ? comparison : -comparison;
                    }
                }

                throw new InvalidOperationException("Shouldn't reach this point");
            }
        }
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

        #region Internal Properties
        internal Table QueryTable => _innerState.QueryTable;

        internal QueryPredicate Predicate => _innerState.Predicate;

        internal IImmutableList<SortColumn> SortColumns => _innerState.SortColumns;

        internal int? TakeCount => _innerState.TakeCount;
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

        public TableQuery WithSortColumns(params IEnumerable<SortColumn> sortColumns)
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

        #region IEnumerable<T>
        IEnumerator<ReadOnlyMemory<object?>> IEnumerable<ReadOnlyMemory<object?>>.GetEnumerator()
        {
            if (_innerState.ProjectionColumnIndexes.Count > 0)
            {
                return ExecuteQueryWithBlockTrace()
                    .Select(r => r.Result)
                    .GetEnumerator();
            }
            else
            {
                throw new InvalidOperationException("No columns would be projected.");
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            if (_innerState.ProjectionColumnIndexes.Count > 0)
            {
                return ExecuteQueryWithBlockTrace()
                    .Select(r => r.Result)
                    .GetEnumerator();
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

        internal IEnumerable<BlockTracedResult<ReadOnlyMemory<object?>>> ExecuteQueryWithBlockTrace()
        {
            var results = _innerState.QueryTable.Database.EnumeratesWithinTransactionContext(
                _innerState.Tx,
                tx =>
                {
                    if (_innerState.TakeCount == 0
                    || (_innerState.InTxOnly && _innerState.CommittedOnly))
                    {
                        return Array.Empty<BlockTracedResult<ReadOnlyMemory<object?>>>();
                    }
                    else
                    {
                        var blockTraceList = CreateBlockTraceList();

                        return ExecuteQuery(blockTraceList, tx);
                    }
                });

            return results;
        }

        public long Count()
        {
            //  Project no columns & sort no columns
            var items = WithProjection().ExecuteQueryWithBlockTrace();
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
                        var deletedRecordIds = WithProjection(
                            _innerState.QueryTable.Schema.RecordIdColumnIndex,
                            _innerState.QueryTable.Schema.ParentBlockIdColumnIndex)
                        .WithSortColumns()
                        .ExecuteQuery(CreateBlockTraceList(), tx)
                        .Select(r => (long)r.Result.Span[0]!)
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

        #region Query Logic
        private static List<BlockTrace> CreateBlockTraceList()
        {
            return new List<BlockTrace>(32);
        }

        private IEnumerable<BlockTracedResult<ReadOnlyMemory<object?>>> ExecuteQuery(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            if (_innerState.SortColumns.Count == 0)
            {
                return ExecuteQueryWithoutSort(blockTraceList, tx);
            }
            else
            {
                return ExecuteQueryWithSort(blockTraceList, tx);
            }
        }

        #region List Blocks
        private IEnumerable<BlockTracedResult<IBlock>> ListBlocks(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            if (_innerState.InMemoryOnly || _innerState.InTxOnly)
            {
                return ListUnpersistedBlocks(blockTraceList, tx);
            }
            else
            {
                return ListUnpersistedBlocks(blockTraceList, tx)
                    .Concat(ListPersistedBlocks(blockTraceList, tx));
            }
        }

        private IEnumerable<BlockTracedResult<IBlock>> ListUnpersistedBlocks(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            var transactionState = tx.TransactionState;
            var blockTraceIndex = blockTraceList.Count;
            var blockId = 0;

            CollectionsMarshal.SetCount(blockTraceList, blockTraceIndex + 1);
            foreach (var block in
                transactionState.ListBlocks(
                    _innerState.QueryTable.Schema.TableName,
                    _innerState.InTxOnly,
                    _innerState.CommittedOnly))
            {
                blockTraceList[blockTraceIndex] = new BlockTrace(_innerState.QueryTable.Schema, blockId--);

                yield return new BlockTracedResult<IBlock>(blockTraceList, block);
            }
            CollectionsMarshal.SetCount(blockTraceList, blockTraceIndex);
        }

        private IEnumerable<BlockTracedResult<IBlock>> ListPersistedBlocks(List<BlockTrace> blockTraceList, TransactionContext tx)
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
                    .WithPredicate(metaPredicate)
                    .WithProjection(metaSchema.BlockIdColumnIndex);

                if (_innerState.QueryTag != null)
                {
                    metaDataQuery = metaDataQuery.WithQueryTag(_innerState.QueryTag);
                }
                foreach (var result in metaDataQuery.ExecuteQueryWithBlockTrace())
                {
                    var blockTraceIndex = blockTraceList.Count;
                    var blockId = (int)result.Result.Span[0]!;

                    CollectionsMarshal.SetCount(blockTraceList, blockTraceIndex + 1);
                    blockTraceList[blockTraceIndex] = new BlockTrace(
                        _innerState.QueryTable.Schema,
                        blockId);
                    yield return new BlockTracedResult<IBlock>(
                        blockTraceList,
                        _innerState.QueryTable.Database.GetOrLoadBlock(
                            blockId,
                            _innerState.QueryTable.Schema));
                    CollectionsMarshal.SetCount(blockTraceList, blockTraceIndex);
                }
            }
        }
        #endregion

        #region Query without sort
        private IEnumerable<BlockTracedResult<ReadOnlyMemory<object?>>> ExecuteQueryWithoutSort(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
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
            var buffer = new object?[_innerState.ProjectionColumnIndexes.Count].AsMemory();
            var queryId = Guid.NewGuid().ToString();

            predicate = predicate.Simplify() ?? predicate;
            foreach (var tracedBlock in ListBlocks(blockTraceList, tx))
            {
                var filterOutput = tracedBlock.Result.Filter(
                    predicate,
                    _innerState.QueryTag != null);
                var blockId = tracedBlock.BlockTraces.Last().BlockId;
                var results = tracedBlock.Result.Project(
                    buffer,
                    _innerState.ProjectionColumnIndexes,
                    filterOutput.RowIndexes,
                    blockId);

                AuditPredicate(filterOutput.PredicateAuditTrails, blockId, queryId);
                foreach (var result in results)
                {
                    yield return new BlockTracedResult<ReadOnlyMemory<object?>>(
                        blockTraceList,
                        result);
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
        #endregion

        #region Query with sort
        private IEnumerable<BlockTracedResult<ReadOnlyMemory<object?>>> ExecuteQueryWithSort(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            //  First phase sort + truncate sort columns
            var blockRowIndexes = SortAndTruncateSortColumns(tx)
                .ToImmutableArray();
            //  Second phase:  re-query blocks to project results
            throw new NotImplementedException();
            //var materializedProjectionColumnIndexes = _innerState.ProjectionColumnIndexes
            //    .Append(_innerState.QueryTable.Schema.RecordIndexColumnIndex)
            //    .ToImmutableArray();
            //var buffer = new object?[materializedProjectionColumnIndexes.Length].AsMemory();

            //while (sortedResults.Any())
            //{
            //    var firstBlockId = sortedResults.First().BlockRowIndex.BlockId;
            //    var rowIndexes = sortedResults
            //        .Where(r => r.BlockRowIndex.BlockId == firstBlockId)
            //        .Select(r => r.BlockRowIndex.RowIndex);
            //    var block = GetBlock(tx, firstBlockId);
            //    var resultMap = block.Project(
            //        buffer,
            //        materializedProjectionColumnIndexes,
            //        rowIndexes,
            //        firstBlockId)
            //        .ToImmutableDictionary(
            //        r => ((int)(r.Span[materializedProjectionColumnIndexes.Length - 1])!),
            //        r => r.Slice(0, materializedProjectionColumnIndexes.Length - 1).ToArray());
            //    //  Resolve result and store them in reverse order for optimal deletion
            //    var newSortedResults = sortedResults
            //        .Select(r => r.BlockRowIndex.BlockId == firstBlockId
            //        ? new SortedResult(r.BlockRowIndex, resultMap[r.BlockRowIndex.RowIndex])
            //        : r)
            //        .Reverse()
            //        .ToList();

            //    //  Return available results
            //    while (newSortedResults.Any() && newSortedResults.Last().Result != null)
            //    {
            //        yield return newSortedResults.Last().Result!.Value;
            //        newSortedResults.RemoveAt(newSortedResults.Count() - 1);
            //    }
            //    //  Put the sequence in correct order
            //    sortedResults = newSortedResults.AsEnumerable().Reverse().ToImmutableArray();
            //}
        }

        private IEnumerable<BlockRowIndex> SortAndTruncateSortColumns(TransactionContext tx)
        {
            var blockTraceList = CreateBlockTraceList();
            var projectionColumns = _innerState.SortColumns
                .Select(s => s.ColumnIndex)
                //  Bring record index
                .Append(_innerState.QueryTable.Schema.RecordIndexColumnIndex);
            var sortQuery = this
                .WithProjection(projectionColumns)
                .WithSortColumns()
                .WithTake(null);
            var accumulatedSortValues = new List<SortedResult>();
            var comparer = new SortComparer(_innerState.SortColumns.Select(s => s.IsAscending));

            foreach (var result in sortQuery.ExecuteQuery(blockTraceList, tx))
            {
                var recordIndex = (int)result.Result.Span[result.Result.Length - 1]!;
                var tempSortedResult = new SortedResult(
                    new BlockRowIndex(blockTraceList, recordIndex),
                    result.Result.Slice(0, result.Result.Length - 1));

                if (accumulatedSortValues.Count == 0 || _innerState.TakeCount == null)
                {   //  If no take we sort at the end
                    accumulatedSortValues.Add(tempSortedResult with
                    {
                        Result = tempSortedResult.Result.ToArray()
                    });
                }
                else
                {
                    var index = accumulatedSortValues.BinarySearch(tempSortedResult, comparer);

                    index = index < 0 ? ~index : index;
                    // Only insert if it belongs in top N results
                    if (index < _innerState.TakeCount)
                    {
                        accumulatedSortValues.Insert(
                            index,
                            tempSortedResult with
                            {
                                Result = tempSortedResult.Result.ToArray()
                            });

                        // Trim if over limit
                        if (accumulatedSortValues.Count > _innerState.TakeCount)
                        {
                            accumulatedSortValues.RemoveAt(accumulatedSortValues.Count - 1);
                        }
                    }
                }
            }
            if (_innerState.TakeCount == null)
            {   //  We sort at the end
                accumulatedSortValues.Sort(comparer);
            }

            return accumulatedSortValues
                .Select(s => s.BlockRowIndex);
        }
        #endregion
        #endregion
    }
}