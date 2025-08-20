using Ipdb.Lib2.Cache;
using Ipdb.Lib2.Cache.CachedBlock;
using Ipdb.Lib2.DbStorage;
using Ipdb.Lib2.Query;
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

namespace Ipdb.Lib2
{
    /// <summary>
    /// Database:  a collection of tables that can share transactions
    /// and are persisted in the same file.
    /// </summary>
    public class Database : IAsyncDisposable
    {
        #region MyRegion
        private record TombstoneRow(long RecordId, long? BlockId, string TableName);
        #endregion

        private const int MAX_IN_MEMORY_SIZE = 5 * 4 * 1024;

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

        #region Constructors
        public Database(
            DatabaseSettings databaseSettings,
            params IEnumerable<TableSchema> schemas)
        {
            _storageManager =
                new Lazy<StorageManager>(() => new StorageManager(Path.GetTempFileName()));
            _userTableMap = schemas
                .Select(s => new
                {
                    Table = CreateTable(s),
                    s.TableName
                })
                .ToImmutableDictionary(o => o.TableName, o => o.Table);

            var invalidTableName = _userTableMap.Keys.FirstOrDefault(name => name.Contains("$"));

            if (invalidTableName != null)
            {
                throw new ArgumentException(
                    $"Table name '{invalidTableName}' is invalid",
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

        internal StorageManager StorageManager => _storageManager.Value;

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
                        blockBuilder.DeleteRecords(deletedRecordIds));
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
                        blockBuilder.DeleteRecords(actuallyDeletedRecordIds));
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
                
                throw new NotImplementedException();
            }

            return false;
        }

        private bool ShouldPersistCachedData(bool doPersistEverything, DatabaseState state)
        {
            var totalRecords = state.DatabaseCache.TableTransactionLogsMap.Values
                .Sum(logs => logs.InMemoryBlocks.Sum(b => b.RecordCount));

            return totalRecords > DatabaseSettings.MaxCachedRecords
                || (doPersistEverything && totalRecords > 0);
        }

        private string GetOldestTable(DatabaseState state)
        {
            var oldestRecordId = long.MaxValue;
            var oldestTableName = string.Empty;

            foreach (var pair in state.DatabaseCache.TableTransactionLogsMap)
            {
                var tableName = pair.Key;
                var logs = pair.Value;

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

            return oldestTableName;
        }
        #endregion
    }
    #endregion
}