using Ipdb.Lib2.Cache;
using Ipdb.Lib2.DbStorage;
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
        private readonly Lazy<StorageManager> _storageManager;
        private readonly IImmutableDictionary<string, Table> _tableMap
            = ImmutableDictionary<string, Table>.Empty;
        private readonly Task _dataMaintenanceTask;
        private readonly ConcurrentQueue<Task> _dataMaintenanceSubTasks = new();
        private readonly TaskCompletionSource _dataMaintenanceStopSource =
            new TaskCompletionSource();
        private TaskCompletionSource _dataMaintenanceTriggerSource = new TaskCompletionSource();
        private TaskCompletionSource? _persistEverythingSource = null;
        private long _recordId = 0;
        private volatile DatabaseState _databaseState = new();

        #region Constructors
        public Database(params IEnumerable<TableSchema> schemas)
        {
            _storageManager =
                new Lazy<StorageManager>(() => new StorageManager(Path.GetTempFileName()));
            _tableMap = schemas
                .Select(s => new
                {
                    Table = CreateTable(s),
                    s.TableName
                })
                .ToImmutableDictionary(o => o.TableName, o => o.Table);
            _dataMaintenanceTask = DataMaintanceAsync();
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
            if (_tableMap.ContainsKey(tableName))
            {
                var table = _tableMap[tableName];

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
        public long NewRecordId()
        {
            return Interlocked.Increment(ref _recordId);
        }

        public IImmutableList<long> NewRecordIds(int recordCount)
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

            var transactionContext = new TransactionContext(this);

            ChangeDatabaseState(currentDbState =>
            {
                var newTransactionMap = currentDbState.TransactionMap.Add(
                    transactionContext.TransactionId,
                    new TransactionCache(
                        currentDbState.DatabaseCache,
                        new TransactionLog()));

                return new DatabaseState(currentDbState.DatabaseCache, newTransactionMap);
            });

            return transactionContext;
        }

        internal void ExecuteWithinTransactionContext(
            TransactionContext? transactionContext,
            Action<TransactionCache> action)
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
            Func<TransactionCache, T> func)
        {
            var temporaryTransactionContext = transactionContext == null
                ? CreateTransaction()
                : null;

            try
            {
                var transactionId = transactionContext?.TransactionId
                    ?? temporaryTransactionContext!.TransactionId;
                var transactionCache = _databaseState.TransactionMap[transactionId];
                var result = func(transactionCache);

                temporaryTransactionContext?.Complete();

                return result;
            }
            catch
            {
                temporaryTransactionContext?.Rollback();
                throw;
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
                    var state = MergeTransactionLogs();

                    if (!PersistOldRecords(state))
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

        private DatabaseState MergeTransactionLogs()
        {
            var isChanging = false;

            var newState = ChangeDatabaseState(state =>
            {
                var cache = state.DatabaseCache;

                //  Merge one table at the time, to avoid racing conditions
                foreach (var pair in cache.TableTransactionLogsMap)
                {
                    var tableName = pair.Key;
                    var logs = pair.Value;

                    if (logs.Logs.Count > 1)
                    {
                        var newCache = new DatabaseCache(
                            cache.StorageBlockMap,
                            cache.TableTransactionLogsMap
                            .SetItem(tableName, logs.MergeLogs()));

                        isChanging = true;

                        return new DatabaseState(newCache, state.TransactionMap);
                    }
                }

                return state;
            });

            if (isChanging)
            {
                return MergeTransactionLogs();
            }
            else
            {   //  Here we should have only one transaction log
                return newState;
            }
        }

        private bool PersistOldRecords(DatabaseState state)
        {
            throw new NotImplementedException();
        }
    }
    #endregion
}