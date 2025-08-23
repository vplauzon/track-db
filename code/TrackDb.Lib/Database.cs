using TrackDb.Lib.Cache;
using TrackDb.Lib.Cache.CachedBlock;
using TrackDb.Lib.DbStorage;
using TrackDb.Lib.Query;
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
        private TaskCompletionSource? _persistEverythingSource = null;
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
                    TableName = t.Schema.TableName,
                    ColumnName = c.ColumnName
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

        internal async Task ForceDataManagementAsync(bool persistAll = false)
        {
            if (persistAll)
            {
                _persistEverythingSource = new TaskCompletionSource();
                _dataMaintenanceTriggerSource.TrySetResult();
                await _persistEverythingSource.Task;
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
                    new TransactionState(
                        currentDbState.DatabaseCache,
                        new TransactionLog()));

                return new DatabaseState(currentDbState.DatabaseCache, newTransactionMap);
            });

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

                var subTask = Task.Run(() => DataMaintanceIteration());

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

        private void DataMaintanceIteration()
        {
            try
            {
                var doPersistEverything = _persistEverythingSource != null;

                while (!_dataMaintenanceStopSource.Task.IsCompleted)
                {
                    var state = MergeTransactionLogsToCanonical();

                    if (!PersistOldRecords(state, doPersistEverything))
                    {   //  We're done
                        _persistEverythingSource?.TrySetResult();

                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                _persistEverythingSource?.TrySetException(ex);
            }
        }

        #region Merge Transaction Logs
        private DatabaseState MergeTransactionLogsToCanonical()
        {
            var newState = MergeTransactionLogs();

            if (newState.DatabaseCache.TableTransactionLogsMap.Values.All(
                l => l.InMemoryBlocks.Count() == 1))
            {
                return newState;
            }
            else
            {
                return MergeTransactionLogsToCanonical();
            }
        }

        private DatabaseState MergeTransactionLogs()
        {
            using (var transactionContext = CreateTransaction())
            {
                var mergedCache = new DatabaseCache(MergeTransactionLogs(transactionContext));
                //  Push merges to db state
                var newState = ChangeDatabaseState(state =>
                {
                    var currentCache = state.DatabaseCache;
                    var truncatedCache =
                        currentCache.RemovePrefixes(transactionContext.TransactionState.DatabaseCache);
                    var resultingCache = mergedCache.Append(truncatedCache);

                    return new DatabaseState(resultingCache, state.TransactionMap);
                });

                transactionContext.Complete();

                return newState;
            }
        }

        private IImmutableDictionary<string, ImmutableTableTransactionLogs> MergeTransactionLogs(
            TransactionContext transactionContext)
        {
            var initialMap = transactionContext.TransactionState.DatabaseCache.TableTransactionLogsMap;
            var mapBuilder = ImmutableDictionary<string, ImmutableTableTransactionLogs>.Empty.ToBuilder();
            var actuallyDeletedRecordIds = new List<long>();

            //  Merge all tables but delete
            foreach (var pair in initialMap.Remove(_tombstoneTable.Schema.TableName))
            {
                var tableName = pair.Key;
                var logs = pair.Value;
                var deletedRecordIds = GetDeletedRecordIds(tableName, transactionContext)
                    .ToImmutableArray();

                if (logs.InMemoryBlocks.Count > 1 || deletedRecordIds.Any())
                {
                    var blockBuilder = logs.MergeLogs();

                    actuallyDeletedRecordIds.AddRange(
                        blockBuilder.DeleteRecordsByRecordId(deletedRecordIds));
                    if (((IBlock)blockBuilder).RecordCount > 0)
                    {
                        mapBuilder.Add(tableName, new ImmutableTableTransactionLogs(blockBuilder));
                    }
                }
                else
                {
                    mapBuilder.Add(tableName, logs);
                }
            }
            //  Process tombstone table
            if (transactionContext.TransactionState.DatabaseCache.TableTransactionLogsMap.TryGetValue(
                _tombstoneTable.Schema.TableName,
                out var tombstoneLogs))
            {
                if (tombstoneLogs.InMemoryBlocks.Count > 1 || actuallyDeletedRecordIds.Any())
                {
                    var blockBuilder = tombstoneLogs.MergeLogs();

                    actuallyDeletedRecordIds.AddRange(
                        blockBuilder.DeleteRecordsByRecordId(actuallyDeletedRecordIds));
                    if (((IBlock)blockBuilder).RecordCount > 0)
                    {
                        mapBuilder.Add(
                            _tombstoneTable.Schema.TableName,
                            new ImmutableTableTransactionLogs(blockBuilder));
                    }
                }
                else
                {
                    mapBuilder.Add(_tombstoneTable.Schema.TableName, tombstoneLogs);
                }
            }
            //  Validate no unpersisted delete in tombstone
            if (_tombstoneTable.Query()
                .Where(ts => ts.BlockId == null)
                .Count() > 0)
            {
                throw new InvalidOperationException("Tombstone is corrupted after merge");
            }

            return mapBuilder.ToImmutableDictionary();
        }
        #endregion

        #region Persist old records
        private bool PersistOldRecords(DatabaseState state, bool doPersistEverything)
        {
            if (ShouldPersistCachedData(doPersistEverything, state))
            {
                var tableName = GetOldestTable(state);
                var logs = state.DatabaseCache.TableTransactionLogsMap[tableName];
                var inMemoryBlock = logs.InMemoryBlocks.First();
                var blockBuilder = new BlockBuilder(inMemoryBlock.TableSchema);
                var metadataTable = GetMetaDataTable(inMemoryBlock.TableSchema);
                var metadataBlockBuilder = new BlockBuilder(metadataTable.Schema);
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
                        metadataBlockBuilder.AppendRecord(
                            NewRecordId(),
                            serializedBlock.MetaData.CreateMetaDataRecord(blockId));
                        isFirstBlock = false;
                    }
                }

                CommitPersistance(blockBuilder, metadataBlockBuilder);

                return true;
            }

            return false;
        }

        private void CommitPersistance(
            BlockBuilder blockBuilder,
            BlockBuilder metadataBlockBuilder)
        {
            ChangeDatabaseState(state =>
            {
                IBlock block = blockBuilder;
                var tableName = block.TableSchema.TableName;
                IBlock metaDataBlock = metadataBlockBuilder;
                var metaDataTableName = metaDataBlock.TableSchema.TableName;
                var map = state.DatabaseCache.TableTransactionLogsMap;

                if (map.TryGetValue(tableName, out var tableLogs))
                {
                    var inMemoryBlocks = tableLogs.InMemoryBlocks;

                    if (block.RecordCount > 0)
                    {
                        inMemoryBlocks = inMemoryBlocks.SetItem(0, block);
                    }
                    else
                    {
                        inMemoryBlocks = inMemoryBlocks.RemoveAt(0);
                    }
                    if (inMemoryBlocks.Any())
                    {
                        map = map.SetItem(tableName, new ImmutableTableTransactionLogs(inMemoryBlocks));
                    }
                    else
                    {
                        map = map.Remove(tableName);
                    }
                    if (map.TryGetValue(metaDataTableName, out var metaDataTableLogs))
                    {
                        var metaDataInMemoryBlocks = metaDataTableLogs.InMemoryBlocks.Add(metaDataBlock);

                        map = map.SetItem(
                            metaDataTableName,
                            new ImmutableTableTransactionLogs(metaDataInMemoryBlocks));
                    }
                    else
                    {
                        map = map.SetItem(
                            metaDataTableName,
                            new ImmutableTableTransactionLogs(metadataBlockBuilder));
                    }

                    return new DatabaseState(new DatabaseCache(map), state.TransactionMap);
                }
                else
                {
                    throw new InvalidOperationException($"Can't find table '{tableName}' in cache");
                }
            });
        }

        private bool ShouldPersistCachedData(bool doPersistEverything, DatabaseState state)
        {
            var totalRecords = state.DatabaseCache.TableTransactionLogsMap.Values
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));
            var totalUserRecords = state.DatabaseCache.TableTransactionLogsMap
                .Where(p => _userTableMap.Keys.Contains(p.Key))
                .Select(p => p.Value)
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));
            var metaDataTableOver = state.DatabaseCache.TableTransactionLogsMap
                .Where(p => _tableToMetaDataTableMap.Keys.Contains(p.Key))
                .Select(p => p.Value)
                .Select(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount))
                .Where(sum => sum > DatabaseSettings.MaxMetaDataCachedRecordsPerTable);

            return totalRecords > DatabaseSettings.MaxCachedRecordsPerDb
                || metaDataTableOver.Any()
                || (doPersistEverything && totalUserRecords > 0);
        }

        private string GetOldestTable(DatabaseState state)
        {
            var oldestRecordId = long.MaxValue;
            var oldestTableName = string.Empty;

            foreach (var pair in state.DatabaseCache.TableTransactionLogsMap)
            {
                var tableName = pair.Key;
                var logs = pair.Value;
                var isMetaData = _tableToMetaDataTableMap.Keys.Contains(tableName);
                //  Tombstone table should never be persisted
                var isTombstoneTable = _tombstoneTable.Schema.TableName == tableName;
                var isTableElligible = !isTombstoneTable
                    && (!isMetaData
                    || logs.InMemoryBlocks.Sum(b => b.RecordCount)
                    > DatabaseSettings.MaxMetaDataCachedRecordsPerTable);

                if (isTableElligible)
                {
                    foreach (var block in logs.InMemoryBlocks)
                    {
                        var blockOldestRecordId = block
                            .Query(AllInPredicate.Instance, new[] { block.TableSchema.Columns.Count })
                            .Select(r => ((long?)r.Span[0])!.Value)
                            .Min();

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
    }
    #endregion
}