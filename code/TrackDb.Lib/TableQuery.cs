using Azure.Storage.Blobs.Models;
using System;
using System.Collections;
using System.Collections.Frozen;
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

        private record BlockWithTrace(
            IReadOnlyList<BlockTrace> BlockTraces,
            BlockTombstones? BlockTombstones,
            int BlockId,
            IBlock Block);

        private record BlockRowIndex(int BlockId, int RowIndex);

        private record BlockTraceRowIndex(IReadOnlyList<BlockTrace> BlockTraces, int RowIndex);

        private record SortedResult(
            BlockTraceRowIndex BlockRowIndex,
            ReadOnlyMemory<object?> Result);

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

                return 0;
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

        internal IEnumerable<BlockTracedResult> ExecuteQueryWithBlockTrace()
        {
            var results = _innerState.QueryTable.Database.EnumeratesWithinTransactionContext(
                _innerState.Tx,
                tx =>
                {
                    if (_innerState.TakeCount == 0
                    || (_innerState.InTxOnly && _innerState.CommittedOnly))
                    {
                        return Array.Empty<BlockTracedResult>();
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

            if (_innerState.QueryTable.Schema.IsMetadata)
            {
                throw new NotSupportedException("Delete isn't supported with metadata tables");
            }
            else
            {
                var dataSchema = (DataTableSchema)_innerState.QueryTable.Schema;

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

                            //  Fetch record IDs of records matching query
                            var deletedRecordTraces = WithProjection(dataSchema.RecordIdColumnIndex)
                            .WithSortColumns()
                            .ExecuteQueryWithBlockTrace()
                            .Select(btr => (
                                RecordId: (long)btr.Result.Span[0]!,
                                btr.BlockTraces.Last().BlockId))
                            .Select(p => (
                                p.RecordId,
                                BlockId: p.BlockId <= 0 ? (int?)null : p.BlockId))
                            .ToFrozenDictionary(p => p.RecordId, p => p.BlockId);
                            //  Hard delete the records that are uncommitted ONLY
                            var remainingDeletedRecordIds = HardDeleteWithinTransaction(
                                transactionTableLog?.NewDataBlock,
                                deletedRecordTraces.Keys);
                            var availabilityBlockManager =
                            _innerState.QueryTable.Database.AvailabilityBlockManager;

                            foreach (var recordId in remainingDeletedRecordIds)
                            {
                                var blockId = deletedRecordTraces[recordId];

                                _innerState.QueryTable.Database.DeleteRecord(
                                    recordId,
                                    _innerState.QueryTable.Schema.TableName,
                                    blockId,
                                    blockId != null
                                    ? availabilityBlockManager.GetBlockVersion(blockId.Value, tx)
                                    : 0,
                                    tx);
                            }

                            return deletedRecordTraces.Count;
                        }
                        else
                        {
                            return 0;
                        }
                    });
            }
        }

        private IEnumerable<long> HardDeleteWithinTransaction(
            BlockBuilder? newDataBlock,
            IEnumerable<long> recordIds)
        {
            if (newDataBlock == null)
            {
                return recordIds;
            }
            else
            {
                IBlock block = newDataBlock;
                var schema = (DataTableSchema)block.TableSchema;
                var predicate = new InPredicate<long>(
                    schema.RecordIdColumnIndex,
                    recordIds,
                    true);
                var rowIndexes = block.Filter(predicate, false).RowIndexes;

                if (rowIndexes.Count > 0)
                {
                    var hardDeletedRecordIds = block.Project(
                        new object?[1],
                        [schema.RecordIdColumnIndex],
                        rowIndexes)
                        .Select(r => (long)r.Span[0]!);
                    var remainingRecordIds = predicate.Values
                        .Except(hardDeletedRecordIds)
                        .ToArray();

                    newDataBlock.DeleteRecordsByRecordIndex(rowIndexes);

                    return remainingRecordIds;
                }
                else
                {
                    return recordIds;
                }
            }
        }

        #region Query Logic
        private static List<BlockTrace> CreateBlockTraceList()
        {
            return new List<BlockTrace>(32);
        }

        private IEnumerable<BlockTracedResult> ExecuteQuery(
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
        private IEnumerable<BlockWithTrace> ListBlocks(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            if (_innerState.InMemoryOnly || _innerState.InTxOnly)
            {
                return ListUnpersistedBlocks(blockTraceList, tx);
            }
            else
            {
                var unpersistedBlocks = ListUnpersistedBlocks(blockTraceList, tx);
                var persistedBlocks = ListPersistedBlocks(blockTraceList, tx);

                return unpersistedBlocks.Concat(persistedBlocks);
            }
        }

        private IEnumerable<BlockWithTrace> ListUnpersistedBlocks(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            var transactionState = tx.TransactionState;
            var blockTraceIndex = blockTraceList.Count;
            var blockId = 0;

            foreach (var block in
                transactionState.ListBlocks(
                    _innerState.QueryTable.Schema.TableName,
                    _innerState.InTxOnly,
                    _innerState.CommittedOnly))
            {
                yield return new BlockWithTrace(
                    blockTraceList,
                    null,
                    blockId--,
                    block);
            }
        }

        private IEnumerable<BlockWithTrace> ListPersistedBlocks(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
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
                var blockTombstonesIndex =
                    tx.TransactionState.UncommittedTransactionLog.ReplacingBlockTombstonesIndex
                    ?? (IDictionary<int, BlockTombstones>)tx.TransactionState.InMemoryDatabase.BlockTombstonesIndex;

                if (_innerState.QueryTag != null)
                {
                    metaDataQuery = metaDataQuery.WithQueryTag(_innerState.QueryTag);
                }
                if (_innerState.IgnoreDeleted)
                {
                    metaDataQuery = metaDataQuery.WithIgnoreDeleted();
                }
                foreach (var result in metaDataQuery.ExecuteQuery(blockTraceList, tx))
                {
                    var blockTraceIndex = blockTraceList.Count;
                    var blockId = (int)result.Result.Span[0]!;
                    BlockTombstones? blockTombstones = null;

                    blockTombstonesIndex.TryGetValue(blockId, out blockTombstones);
                    if (_innerState.IgnoreDeleted
                        || blockTombstones == null
                        || !blockTombstones.IsAllDeleted)
                    {
                        yield return new BlockWithTrace(
                            blockTraceList,
                            blockTombstones,
                            blockId,
                            _innerState.QueryTable.Database.GetOrLoadBlock(
                                blockId,
                                _innerState.QueryTable.Schema));
                    }
                }
            }
        }

        private IBlock GetBlock(int blockId, TransactionContext tx)
        {
            if (blockId <= 0)
            {
                var blockIndex = -blockId;
                var inMemoryBlocks = tx.TransactionState.ListBlocks(
                    _innerState.QueryTable.Schema.TableName,
                    _innerState.InTxOnly,
                    _innerState.CommittedOnly);
                var block = inMemoryBlocks.ElementAt(blockIndex);

                return block;
            }
            else
            {
                return _innerState.QueryTable.Database.GetOrLoadBlock(
                    blockId,
                    _innerState.QueryTable.Schema);
            }
        }
        #endregion

        #region Query without sort
        private IEnumerable<BlockTracedResult> ExecuteQueryWithoutSort(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            var takeCount = _innerState.TakeCount ?? int.MaxValue;
            var schema = _innerState.QueryTable.Schema;
            var dataSchema = schema as DataTableSchema;
            var isTableMeta = dataSchema == null;
            var isTableTombstone =
                schema.TableName == _innerState.QueryTable.Database.TombstoneTable.Schema.TableName;
            var predicate = _innerState.IgnoreDeleted || isTableMeta || isTableTombstone
                ? _innerState.Predicate
                //  Remove deleted records
                : new ConjunctionPredicate(
                    _innerState.Predicate,
                    new InPredicate<long>(
                        dataSchema!.RecordIdColumnIndex,
                        _innerState.QueryTable.Database.GetDeletedRecordIds(schema.TableName, tx),
                        false));
            var buffer = new object?[_innerState.ProjectionColumnIndexes.Count].AsMemory();
            var queryId = Guid.NewGuid().ToString();

            predicate = predicate.Simplify() ?? predicate;
            foreach (var blockWithTrace in ListBlocks(blockTraceList, tx))
            {
                var block = blockWithTrace.Block;
                var filterOutput = block.Filter(predicate, _innerState.QueryTag != null);
                var currentBlockId = blockWithTrace.BlockId;
                var results = block.Project(
                    buffer,
                    _innerState.ProjectionColumnIndexes,
                    filterOutput.RowIndexes);
                var indexedResults = filterOutput.RowIndexes.Zip(
                    results,
                    (i, r) => new
                    {
                        RowIndex = i,
                        Result = r
                    });

                AuditPredicate(filterOutput.PredicateAuditTrails, currentBlockId, queryId);
                foreach (var indexedResult in indexedResults)
                {
                    if (blockWithTrace.BlockTombstones == null
                        || !blockWithTrace.BlockTombstones.IsDeleted(indexedResult.RowIndex))
                    {
                        blockTraceList.Add(new BlockTrace(
                            schema,
                            blockWithTrace.BlockId,
                            blockWithTrace.Block.RecordCount,
                            indexedResult.RowIndex));
                        yield return new BlockTracedResult(blockTraceList, indexedResult.Result);
                        CollectionsMarshal.SetCount(blockTraceList, blockTraceList.Count - 1);
                        --takeCount;
                        if (takeCount == 0)
                        {
                            yield break;
                        }
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
        private IEnumerable<BlockTracedResult> ExecuteQueryWithSort(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            //  First phase sort + truncate sort columns
            var blockTraceRowIndexes = SortAndTruncateSortColumns(blockTraceList, tx)
                .ToArray();
            //  Second phase:  re-query blocks to project results
            var result = new BlockTracedResult[blockTraceRowIndexes.Length];
            var byBlockId = blockTraceRowIndexes
                .Index()
                .Select(p => new
                {
                    ResultRowIndex = p.Index,
                    p.Item.BlockTraces,
                    LastBlockTrace = p.Item.BlockTraces.Last()
                })
                .GroupBy(b => b.LastBlockTrace.BlockId);
            var buffer = new object?[_innerState.ProjectionColumnIndexes.Count];

            //  Fill 'result'
            foreach (var g in byBlockId)
            {
                var blockId = g.Key;
                var block = GetBlock(blockId, tx);
                var records = block.Project(
                    buffer,
                    _innerState.ProjectionColumnIndexes,
                    g.Select(o => o.LastBlockTrace.RowIndex))
                    .Select(r => r.ToArray());
                var zipped = records.Zip(
                    g,
                    (record, groupObject) => new
                    {
                        Record = record,
                        groupObject.ResultRowIndex,
                        groupObject.BlockTraces
                    });

                foreach (var z in zipped)
                {
                    result[z.ResultRowIndex] = new BlockTracedResult(z.BlockTraces, z.Record);
                }
            }

            return result;
        }

        private IEnumerable<BlockTraceRowIndex> SortAndTruncateSortColumns(
            List<BlockTrace> blockTraceList,
            TransactionContext tx)
        {
            var projectionColumns = _innerState.SortColumns
                .Select(s => s.ColumnIndex);
            var sortQuery = this
                .WithProjection(projectionColumns)
                .WithSortColumns()
                .WithTake(null);
            var accumulatedSortValues = new List<SortedResult>();
            var comparer = new SortComparer(_innerState.SortColumns.Select(s => s.IsAscending));

            foreach (var result in sortQuery.ExecuteQuery(blockTraceList, tx))
            {
                var recordIndex = result.BlockTraces.Last().RowIndex;
                var newSortedResult = new SortedResult(
                    new BlockTraceRowIndex(blockTraceList.ToArray(), recordIndex),
                    result.Result.ToArray());

                if (accumulatedSortValues.Count == 0 || _innerState.TakeCount == null)
                {   //  If no take we sort at the end
                    accumulatedSortValues.Add(newSortedResult);
                }
                else
                {
                    var index = accumulatedSortValues.BinarySearch(newSortedResult, comparer);

                    index = index < 0 ? ~index : index;
                    // Only insert if it belongs in top N results
                    if (index < _innerState.TakeCount)
                    {
                        accumulatedSortValues.Insert(index, newSortedResult);

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