using TrackDb.Lib.Cache;
using TrackDb.Lib.Cache.CachedBlock;
using TrackDb.Lib.DbStorage;
using TrackDb.Lib.Predicate;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Settings;

namespace TrackDb.Lib
{
    /// <summary>
    /// Database:  a collection of tables that can share transactions
    /// and are persisted in the same file.
    /// </summary>
    public class Database : IAsyncDisposable
    {
        #region Inner types
        private record TombstoneRow(long RecordId, long? BlockId, string TableName);
        #endregion

        private readonly Lazy<StorageManager> _storageManager;
        private readonly IImmutableDictionary<string, Table> _userTableMap;
        private readonly TypedTable<TombstoneRow> _tombstoneTable;
        private readonly Task _dataMaintenanceTask;
        private readonly ConcurrentQueue<Task> _dataMaintenanceSubTasks = new();
        private readonly TaskCompletionSource _dataMaintenanceStopSource =
            new TaskCompletionSource();
        private TaskCompletionSource _dataMaintenanceTriggerSource = new TaskCompletionSource();
        private DataManagementActivity _dataManagementActivity = DataManagementActivity.None;
        private TaskCompletionSource? _forceDataManagementSource = null;
        private long _recordId = 0;
        private volatile DatabaseState _databaseState = new();
        private volatile IImmutableDictionary<string, Table> _tableToMetaDataTableMap
            = ImmutableDictionary<string, Table>.Empty;

        #region Constructors
        public Database(
            DatabaseSettings databaseSettings,
            params IEnumerable<TableSchema> schemas)
        {
            _storageManager = new Lazy<StorageManager>(
                () => new StorageManager(Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.db")),
                LazyThreadSafetyMode.ExecutionAndPublication);
            _userTableMap = schemas
                .Select(s => new
                {
                    Table = CreateTable(s),
                    s.TableName
                })
                .ToImmutableDictionary(o => o.TableName, o => o.Table);

            var invalidTableName = _userTableMap.Keys.FirstOrDefault(name => name.Contains("$"));
            var invalidColumnName = _userTableMap.Values
                .Select(t => t.Schema.Columns.Select(c => new
                {
                    t.Schema.TableName,
                    c.ColumnName
                }))
                .SelectMany(c => c)
                .FirstOrDefault(o => o.ColumnName.Contains("$"));

            if (invalidTableName != null)
            {
                throw new ArgumentException(
                    $"Table name '{invalidTableName}' is invalid",
                    nameof(schemas));
            }
            if (invalidColumnName != null)
            {
                throw new ArgumentException(
                    $"Table name '{invalidColumnName.TableName}' has invalid column name " +
                    $"'{invalidColumnName.ColumnName}'",
                    nameof(schemas));
            }
            _tombstoneTable = new TypedTable<TombstoneRow>(
                this,
                TypedTableSchema<TombstoneRow>.FromConstructor("$tombstone"));
            _dataMaintenanceTask = DataMaintanceAsync();
            DatabaseSettings = databaseSettings;
        }

        private Table CreateTable(TableSchema schema)
        {
            var schemaType = schema.GetType();

            if (schemaType.IsGenericType
                && schemaType.GetGenericTypeDefinition() == typeof(TypedTableSchema<>))
            {
                var representationType = schemaType.GenericTypeArguments[0];
                var tableType = typeof(TypedTable<>).MakeGenericType(representationType);
                var table = Activator.CreateInstance(
                    tableType,
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    [this, schema],
                    null);

                return (Table)table!;
            }
            else
            {
                return new Table(this, schema);
            }
        }
        #endregion

        public DatabaseSettings DatabaseSettings { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _dataMaintenanceStopSource.SetResult();
            await _dataMaintenanceTask;
            while (_dataMaintenanceSubTasks.TryDequeue(out var subTask))
            {
                subTask.Wait();
            }
            if (_storageManager.IsValueCreated)
            {
                ((IDisposable)_storageManager.Value).Dispose();
            }
        }

        public Table GetTable(string tableName)
        {
            if (_userTableMap.ContainsKey(tableName))
            {
                var table = _userTableMap[tableName];

                return table;
            }
            else
            {
                throw new InvalidOperationException($"Table '{tableName}' doesn't exist");
            }
        }

        public TypedTable<T> GetTypedTable<T>(string tableName)
            where T : notnull
        {
            var table = GetTable(tableName);

            if (table is TypedTable<T> t)
            {
                return t;
            }
            else if (table.GetType() == typeof(Table))
            {
                throw new InvalidOperationException($"Table '{tableName}' is a non-typed table");
            }
            else
            {
                var docType = table.GetType().GetGenericArguments().First();

                throw new InvalidOperationException(
                    $"Table '{tableName}' doesn't have document type '{typeof(T).Name}':  " +
                    $"it has document type '{docType.Name}'");
            }
        }

        #region Meta data tables
        internal bool IsMetaDataTable(string tableName)
        {
            var existingMap = _tableToMetaDataTableMap;
            var metaDataTableName = GetMetadataTableName(tableName);

            return existingMap.ContainsKey(metaDataTableName);
        }

        internal Table GetMetaDataTable(TableSchema schema)
        {
            var existingMap = _tableToMetaDataTableMap;
            var metaDataTableName = GetMetadataTableName(schema.TableName);

            if (existingMap.TryGetValue(metaDataTableName, out var metaDataTable))
            {
                return metaDataTable;
            }
            else
            {
                var metaDataSchema = CreateMetaDataSchema(metaDataTableName, schema);
                var newMap = existingMap.Add(
                    metaDataSchema.TableName,
                    new Table(this, metaDataSchema));

                Interlocked.CompareExchange(ref _tableToMetaDataTableMap, newMap, existingMap);

                //  Go back to the map in case another thread created the table and won
                return GetMetaDataTable(schema);
            }
        }

        private string GetMetadataTableName(string tableName)
        {
            return $"$meta-{tableName}";
        }

        private TableSchema CreateMetaDataSchema(string metaDataTableName, TableSchema schema)
        {
            var metaDataColumns = schema.Columns
                //  For each column we create a min, max & hasNulls column
                .Select(c => new[]
                {
                    new ColumnSchema($"$hasNulls-{c.ColumnName}", typeof(bool)),
                    new ColumnSchema($"$min-{c.ColumnName}", c.ColumnType),
                    new ColumnSchema($"$max-{c.ColumnName}", c.ColumnType)
                })
                //  We add the extent-id columns
                .Append(new[]
                {
                    new ColumnSchema("$hasNulls-$extentId", typeof(bool)),
                    new ColumnSchema("$min-$extentId", typeof(long)),
                    new ColumnSchema("$max-$extentId", typeof(long))
                })
                //  We add the itemCount & block-id columns
                .Append(new[]
                {
                    new ColumnSchema("$itemCount", typeof(int)),
                    new ColumnSchema("$blockId", typeof(int))
                })
                //  We fan out the columns
                .SelectMany(c => c);
            var metaDataSchema = new TableSchema(
                metaDataTableName,
                metaDataColumns,
                Array.Empty<int>());

            return metaDataSchema;
        }
        #endregion

        internal async Task ForceDataManagementAsync(
            DataManagementActivity dataManagementActivity = DataManagementActivity.None)
        {
            if (dataManagementActivity != DataManagementActivity.None)
            {
                _forceDataManagementSource = new TaskCompletionSource();
                Interlocked.Exchange(ref _dataManagementActivity, dataManagementActivity);
                _dataMaintenanceTriggerSource.TrySetResult();
                await _forceDataManagementSource.Task;
            }
        }

        #region Record IDs
        internal long NewRecordId()
        {
            return Interlocked.Increment(ref _recordId);
        }

        internal IImmutableList<long> NewRecordIds(int recordCount)
        {
            var nextId = Interlocked.Add(ref _recordId, recordCount);

            return Enumerable.Range(0, recordCount)
                .Select(i => i + nextId - recordCount)
                .ToImmutableArray();
        }
        #endregion

        #region Transaction
        public TransactionContext CreateTransaction()
        {
            ObserveSubTasks();

            var transactionContext = new TransactionContext(
                this,
                transactionId =>
                {
                    if (_databaseState.TransactionMap.TryGetValue(transactionId, out var transaction))
                    {
                        return transaction;
                    }
                    else
                    {
                        throw new ArgumentException(
                            "Transaction ID can't be found in database state",
                            nameof(transactionId));
                    }
                });

            ChangeDatabaseState(currentDbState =>
            {
                var newTransactionMap = currentDbState.TransactionMap.Add(
                    transactionContext.TransactionId,
                    new TransactionState(currentDbState.DatabaseCache));

                return new DatabaseState(currentDbState.DatabaseCache, newTransactionMap);
            });

            return transactionContext;
        }

        private TransactionContext CreateDummyTransaction()
        {
            var state = _databaseState;
            var transactionContext = new TransactionContext(
                this,
                new TransactionState(state.DatabaseCache));

            return transactionContext;
        }

        internal void ExecuteWithinTransactionContext(
            TransactionContext? transactionContext,
            Action<TransactionContext> action)
        {
            ExecuteWithinTransactionContext(
                transactionContext,
                tc =>
                {
                    action(tc);

                    return 0;
                });
        }

        internal T ExecuteWithinTransactionContext<T>(
            TransactionContext? transactionContext,
            Func<TransactionContext, T> func)
        {
            var temporaryTransactionContext = transactionContext == null
                ? CreateTransaction()
                : null;

            try
            {
                var actualTransactionContext = transactionContext ?? temporaryTransactionContext;
                var result = func(actualTransactionContext!);

                temporaryTransactionContext?.Complete();

                return result;
            }
            catch
            {
                temporaryTransactionContext?.Rollback();
                throw;
            }
        }

        internal IEnumerable<T> EnumeratesWithinTransactionContext<T>(
            TransactionContext? transactionContext,
            Func<TransactionContext, IEnumerable<T>> func)
        {
            var temporaryTransactionContext = transactionContext == null
                ? CreateTransaction()
                : null;

            try
            {
                var actualTransactionContext = transactionContext ?? temporaryTransactionContext;
                var results = func(actualTransactionContext!);

                foreach (var result in results)
                {
                    yield return result;
                }
            }
            finally
            {
                temporaryTransactionContext?.Rollback();
            }
        }

        internal void CompleteTransaction(long transactionId)
        {
            //  Fetch transaction cache
            var transactionCache = _databaseState.TransactionMap[transactionId];

            ChangeDatabaseState(currentDbState =>
            {   //  Remove it from map
                var newTransactionMap = currentDbState.TransactionMap.Remove(transactionId);

                if (transactionCache.UncommittedTransactionLog.IsEmpty)
                {
                    return new DatabaseState(currentDbState.DatabaseCache, newTransactionMap);
                }
                else
                {
                    return new DatabaseState(
                        currentDbState.DatabaseCache.CommitLog(transactionCache.UncommittedTransactionLog),
                        newTransactionMap);
                }
            });
        }

        internal void RollbackTransaction(long transactionId)
        {
            ChangeDatabaseState(currentDbState =>
            {   //  Remove transaction from map (and forget about it)
                var newTransactionMap = currentDbState.TransactionMap.Remove(transactionId);

                return new DatabaseState(currentDbState.DatabaseCache, newTransactionMap);
            });
        }

        private DatabaseState ChangeDatabaseState(Func<DatabaseState, DatabaseState?> stateChange)
        {   //  Optimistically try to change the db state:  repeat if necessary
            var currentDbState = _databaseState;
            var newDbState = stateChange(currentDbState);

            if (newDbState == null)
            {
                return currentDbState;
            }
            else if (object.ReferenceEquals(
                currentDbState,
                Interlocked.CompareExchange(ref _databaseState, newDbState, currentDbState)))
            {
                return newDbState;
            }
            else
            {   //  Exchange fail, we retry
                return ChangeDatabaseState(stateChange);
            }
        }
        #endregion

        #region Tombstone
        internal void DeleteRecords(
            IEnumerable<long> recordIds,
            long? blockId,
            string tableName,
            TransactionContext transactionContext)
        {
            var tombstoneRows = recordIds
                .Select(id => new TombstoneRow(id, blockId, tableName));

            _tombstoneTable.AppendRecords(tombstoneRows, transactionContext);
        }

        internal IEnumerable<long> GetDeletedRecordIds(
            string tableName,
            TransactionContext transactionContext)
        {
            return tableName != _tombstoneTable.Schema.TableName
                ? _tombstoneTable.Query(transactionContext)
                .Where(ts => ts.TableName == tableName)
                .Select(ts => ts.RecordId)
                : Array.Empty<long>();
        }

        internal BlockBuilder RemoveFromTombstone(
            string tableName,
            IEnumerable<long> recordIds,
            TransactionContext transactionContext)
        {
            var predicate = _tombstoneTable.PredicateFactory
                .Equal(t => t.TableName, tableName)
                .And(_tombstoneTable.PredicateFactory.In(t => t.RecordId, recordIds));
            var query = new TableQuery(
                _tombstoneTable,
                transactionContext,
                predicate,
                //  Project the row index
                new[] { _tombstoneTable.Schema.Columns.Count() + 1 },
                Array.Empty<SortColumn>(),
                null);
            var tombstoneRecordIndexes = query
                .Select(r => ((int?)r.Span[0])!.Value)
                .ToImmutableArray();
            var tombstoneTableName = _tombstoneTable.Schema.TableName;
            var tombstoneLogs =
                transactionContext.TransactionState.DatabaseCache.TableTransactionLogsMap[tombstoneTableName];
            var tombstoneBlockBuilder = tombstoneLogs.MergeLogs();

            tombstoneBlockBuilder.DeleteRecordsByRecordIndex(tombstoneRecordIndexes);

            return tombstoneBlockBuilder;
        }
        #endregion

        #region Block load
        internal IBlock GetOrLoadBlock(
            int blockId,
            TableSchema schema,
            SerializedBlockMetaData serializedBlockMetaData)
        {
            var payload = _storageManager.Value.ReadBlock(blockId);
            var serializedBlock = new SerializedBlock(serializedBlockMetaData, payload);
            var block = new ReadOnlyBlock(schema, serializedBlock);

            return block;
        }
        #endregion

        #region Data Maintenance
        private async Task DataMaintanceAsync()
        {
            while (!_dataMaintenanceStopSource.Task.IsCompleted)
            {
                await Task.WhenAny(
                    _dataMaintenanceTriggerSource.Task,
                    _dataMaintenanceStopSource.Task);

                //  Reset the trigger source
                _dataMaintenanceTriggerSource = new TaskCompletionSource();

                var dataManagementActivity = Interlocked.Exchange(
                    ref _dataManagementActivity,
                    DataManagementActivity.None);
                var subTask = Task.Run(() => DataMaintanceIteration(dataManagementActivity));

                //  Queue sub task so it can be observed later
                _dataMaintenanceSubTasks.Enqueue(subTask);

                await subTask;
            }
        }

        private void ObserveSubTasks()
        {
            var incompletedTasks = new List<Task>();

            try
            {
                while (_dataMaintenanceSubTasks.TryDequeue(out var subTask))
                {
                    if (!subTask.IsCompleted || subTask.IsFaulted)
                    {
                        incompletedTasks.Add(subTask);
                    }
                    if (subTask.IsCompleted)
                    {   //  Observe task before discarting it
                        subTask.Wait();
                    }
                }
            }
            finally
            {   //  Requeue all incompleted or faulted tasks so they can be observed
                foreach (var task in incompletedTasks)
                {
                    _dataMaintenanceSubTasks.Enqueue(task);
                }
            }
        }

        private void DataMaintanceIteration(DataManagementActivity dataManagementActivity)
        {
            try
            {
                while (!_dataMaintenanceStopSource.Task.IsCompleted)
                {
                    var doMergeAll =
                        (dataManagementActivity & DataManagementActivity.MergeAllInMemoryLogs) != 0;
                    var doPersistAll =
                        (dataManagementActivity & DataManagementActivity.PersistAllData) != 0;
                    var doHardDeleteAll =
                        (dataManagementActivity & DataManagementActivity.HardDeleteAll) != 0;

                    if (MergeTransactionLogs(doMergeAll))
                    {
                        if (PersistOldRecords(doPersistAll))
                        {
                            if (HardDelete(doHardDeleteAll))
                            {   //  We're done
                                _forceDataManagementSource?.TrySetResult();

                                return;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _forceDataManagementSource?.TrySetException(ex);
            }
        }

        #region Merge Transaction Logs
        private bool MergeTransactionLogs(bool doMergeAll)
        {
            var maxInMemoryBlocksPerTable = doMergeAll
                ? 1
                : DatabaseSettings.MaxUnpersistedBlocksPerTable;
            var candidateTableName = _databaseState.DatabaseCache.TableTransactionLogsMap
                .Where(p => p.Value.InMemoryBlocks.Count > maxInMemoryBlocksPerTable)
                .Select(p => p.Key)
                .FirstOrDefault();

            if (candidateTableName != null)
            {
                MergeTransactionLogs(candidateTableName);
                MergeTransactionLogs(doMergeAll);

                return false;
            }
            else
            {
                return true;
            }
        }

        private void MergeTransactionLogs(string tableName)
        {
            (BlockBuilder tableBlock, BlockBuilder? tombstoneBlock) MergeTransactionLogs(
                string tableName,
                TransactionContext tc)
            {
                var dbCache = tc.TransactionState.DatabaseCache;
                var logs = dbCache.TableTransactionLogsMap[tableName];
                var blockBuilder = logs.MergeLogs();
                var deletedRecordsIds = GetDeletedRecordIds(tableName, tc);
                var actuallyDeletedRecordIds = blockBuilder.DeleteRecordsByRecordId(deletedRecordsIds)
                    .ToImmutableArray();

                if (actuallyDeletedRecordIds.Any())
                {   //  We need to erase the tombstones record that were actually deleted
                    var tombstoneBlockBuilder =
                        RemoveFromTombstone(tableName, actuallyDeletedRecordIds, tc);

                    return (blockBuilder, tombstoneBlockBuilder);
                }
                else
                {
                    return (blockBuilder, null);
                }
            }

            ImmutableTableTransactionLogs UpdateLogs(
                ImmutableTableTransactionLogs oldLogs,
                ImmutableTableTransactionLogs currentLogs,
                BlockBuilder block)
            {
                return new ImmutableTableTransactionLogs(currentLogs.InMemoryBlocks
                    .Skip(oldLogs.InMemoryBlocks.Count)
                    .Prepend(block)
                    .ToImmutableArray());
            }

            using (var tc = CreateDummyTransaction())
            {
                (var tableBlock, var tombstoneBlock) = MergeTransactionLogs(tableName, tc);

                ChangeDatabaseState(state =>
                {
                    var map = state.DatabaseCache.TableTransactionLogsMap;

                    map = map.SetItem(
                        tableName,
                        UpdateLogs(
                            tc.TransactionState.DatabaseCache.TableTransactionLogsMap[tableName],
                            map[tableName],
                            tableBlock));
                    if (tombstoneBlock != null)
                    {
                        var tombstoneTableName = _tombstoneTable.Schema.TableName;

                        map = map.SetItem(
                            tombstoneTableName,
                            UpdateLogs(
                                tc.TransactionState.DatabaseCache.TableTransactionLogsMap[tombstoneTableName],
                                map[tombstoneTableName],
                                tombstoneBlock));
                    }

                    return new DatabaseState(new DatabaseCache(map), state.TransactionMap);
                });
            }
        }
        #endregion

        #region Persist old records
        /// <summary>
        /// Returns <c>true</c> when everything that should be persisted was persisted.
        /// </summary>
        /// <param name="doPersistEverything"></param>
        /// <returns></returns>
        private bool PersistOldRecords(bool doPersistEverything)
        {
            using (var tc = CreateDummyTransaction())
            {
                if (ShouldPersistCachedData(doPersistEverything, tc))
                {
                    var tableName = GetOldestTable(tc);

                    if (tableName != null)
                    {
                        var logs = tc.TransactionState.DatabaseCache.TableTransactionLogsMap[tableName];

                        if (logs.InMemoryBlocks.Count > 1)
                        {
                            MergeTransactionLogs(tableName);
                        }
                        else
                        {
                            var inMemoryBlock = logs.InMemoryBlocks.First();
                            var blockBuilder = new BlockBuilder(inMemoryBlock.TableSchema);
                            var metadataTable = GetMetaDataTable(inMemoryBlock.TableSchema);
                            var isFirstBlock = true;

                            blockBuilder.AppendBlock(inMemoryBlock);
                            blockBuilder.OrderByRecordId();

                            while (((IBlock)blockBuilder).RecordCount > 0)
                            {
                                var blockToPersist = blockBuilder.TruncateBlock(_storageManager.Value.BlockSize);
                                var rowCount = ((IBlock)blockToPersist).RecordCount;

                                //  We stop before persisting the last (typically incomplete) block
                                if (isFirstBlock || ((IBlock)blockBuilder).RecordCount == rowCount)
                                {
                                    var serializedBlock = blockToPersist.Serialize();
                                    var blockId = _storageManager.Value.WriteBlock(serializedBlock.Payload.ToArray());

                                    blockBuilder.DeleteRecordsByRecordIndex(Enumerable.Range(0, rowCount));
                                    metadataTable.AppendRecord(
                                        serializedBlock.MetaData.CreateMetaDataRecord(blockId),
                                        tc);
                                    isFirstBlock = false;
                                }
                            }
                            CommitPersistance(blockBuilder, metadataTable, tc);
                        }
                    }

                    return false;
                }
                else
                {
                    return true;
                }
            }
        }

        private void CommitPersistance(
            BlockBuilder blockBuilder,
            Table metadataTable,
            TransactionContext tc)
        {
            ChangeDatabaseState(state =>
            {
                IBlock block = blockBuilder;
                var tableName = block.TableSchema.TableName;
                var oldCache = tc.TransactionState.DatabaseCache;
                var metaDataTableName = metadataTable.Schema.TableName;
                var map = state.DatabaseCache.TableTransactionLogsMap;
                var metaDataBlock =
                    tc.TransactionState.UncommittedTransactionLog.TableBlockBuilderMap[metaDataTableName];
                var metaDataLogs = state.DatabaseCache.TableTransactionLogsMap.ContainsKey(metaDataTableName)
                    ? new ImmutableTableTransactionLogs(
                        state.DatabaseCache.TableTransactionLogsMap[metaDataTableName].InMemoryBlocks
                        .Prepend(metaDataBlock)
                        .ToImmutableArray())
                    : new ImmutableTableTransactionLogs(metaDataBlock);

                //  Remove / update the logs for the table
                map = map.SetItem(tableName, new ImmutableTableTransactionLogs(
                    state.DatabaseCache.TableTransactionLogsMap[tableName].InMemoryBlocks
                    .Skip(oldCache.TableTransactionLogsMap[tableName].InMemoryBlocks.Count)
                    .Prepend(blockBuilder)
                    .ToImmutableArray()));
                //  Add logs for metadata table
                map = map.SetItem(metaDataTableName, metaDataLogs);

                return new DatabaseState(new DatabaseCache(map), state.TransactionMap);
            });
        }

        private bool ShouldPersistCachedData(
            bool doPersistEverything,
            TransactionContext tc)
        {
            var cache = tc.TransactionState.DatabaseCache;
            var totalRecords = cache.TableTransactionLogsMap.Values
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));
            var totalUserRecords = cache.TableTransactionLogsMap
                .Where(p => _userTableMap.Keys.Contains(p.Key))
                .Select(p => p.Value)
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));
            var metaDataTableOver = cache.TableTransactionLogsMap
                .Where(p => _tableToMetaDataTableMap.Keys.Contains(p.Key))
                .Select(p => p.Value)
                .Select(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount))
                .Where(sum => sum > DatabaseSettings.MaxUnpersistedMetaDataRecordsPerTable);

            return totalRecords > DatabaseSettings.MaxUnpersistedRecordsPerDb
                || metaDataTableOver.Any()
                || (doPersistEverything && totalUserRecords > 0);
        }

        private string? GetOldestTable(TransactionContext tc)
        {
            var cache = tc.TransactionState.DatabaseCache;
            var oldestRecordId = long.MaxValue;
            var oldestTableName = (string?)null;
            var buffer = new object?[1].AsMemory();
            var rowIndexes = new[] { 0 };
            var projectedColumns = new int[1];

            foreach (var pair in cache.TableTransactionLogsMap)
            {
                var tableName = pair.Key;
                var logs = pair.Value;
                var isMetaData = _tableToMetaDataTableMap.Keys.Contains(tableName);
                //  Tombstone table should never be persisted
                var isTombstoneTable = _tombstoneTable.Schema.TableName == tableName;
                var isTableElligible = !isTombstoneTable
                    && (!isMetaData
                    || logs.InMemoryBlocks.Sum(b => b.RecordCount)
                    > DatabaseSettings.MaxUnpersistedMetaDataRecordsPerTable);

                if (isTableElligible)
                {
                    var block = logs.InMemoryBlocks
                        .Where(b => b.RecordCount > 0)
                        .FirstOrDefault();

                    if (block != null)
                    {   //  Fetch the record ID
                        projectedColumns[0] = block.TableSchema.Columns.Count;

                        var blockOldestRecordId = block.Project(buffer, projectedColumns, rowIndexes, 0)
                            .Select(r => ((long?)r.Span[0])!.Value)
                            .First();

                        if (blockOldestRecordId < oldestRecordId)
                        {
                            oldestRecordId = blockOldestRecordId;
                            oldestTableName = tableName;
                        }
                    }
                }
            }

            return oldestTableName;
        }
        #endregion

        #region Hard Delete
        private bool HardDelete(bool doHardDeleteAll)
        {
            using (var tc = CreateDummyTransaction())
            {
                return true;
            }
        }
        #endregion
    }
    #endregion
}