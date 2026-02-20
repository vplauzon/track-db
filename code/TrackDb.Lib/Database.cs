using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Logging;
using TrackDb.Lib.Policies;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.Statistics;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    /// <summary>
    /// Database:  a collection of tables that can share transactions
    /// and are persisted in the same file.
    /// </summary>
    public class Database : IAsyncDisposable
    {
        private readonly Lazy<DatabaseFileManager> _dbFileManager;
        private readonly DataLifeCycleManager _dataLifeCycleManager;
        private readonly IImmutableList<TableSchema> _schemaWithTriggers;
        private readonly BlobLock? _blobLock;
        private LogFlushManager? _logFlushManager;
        private DatabaseContextBase? _databaseContext;
        private volatile DatabaseState _databaseState;
        private volatile int _activeTransactionCount = 0;

        #region Constructors
        public async static Task<DATABASE_CONTEXT> CreateAsync<DATABASE_CONTEXT>(
            DatabasePolicy databasePolicies,
            Func<Database, DATABASE_CONTEXT> contextFactory,
            CancellationToken ct,
            params IEnumerable<TableSchema> schemas)
            where DATABASE_CONTEXT : DatabaseContextBase
        {
            var useLogging = databasePolicies.LogPolicy.StorageConfiguration != null;
            var blobClients = useLogging
                ? databasePolicies.LogPolicy.StorageConfiguration!.CreateClients()
                : null;
            var blockLock = useLogging
                ? await BlobLock.CreateAsync(blobClients!, ct)
                : null;
            var database = new Database(databasePolicies, blockLock, schemas);
            var context = contextFactory(database);

            if (context == null)
            {
                throw new NullReferenceException("Database context factory returned null");
            }
            database._databaseContext = context;
            if (useLogging)
            {
                await database.InitLogsAsync(blobClients!, ct);
            }

            return context;
        }

        private Database(
            DatabasePolicy databasePolicies,
            BlobLock? blobLock,
            IEnumerable<TableSchema> schemas)
        {
            var userTables = schemas
                .Select(s => CreateTable(s))
                .ToImmutableArray();
            var localFolder = Path.Combine(Path.GetTempPath(), "track-db", Guid.NewGuid().ToString());

            if (Directory.Exists(localFolder))
            {
                Directory.Delete(localFolder, true);
            }
            Directory.CreateDirectory(localFolder);
            _dbFileManager = new Lazy<DatabaseFileManager>(
                () => new DatabaseFileManager(
                    Path.Combine(localFolder, $"blocks.db"),
                    databasePolicies.StoragePolicy.BlockSize),
                LazyThreadSafetyMode.ExecutionAndPublication);

            var invalidTableName = userTables
                .Select(t => t.Schema.TableName)
                .FirstOrDefault(name => name.Contains("$"));
            var invalidColumnName = userTables
                .Select(t => t.Schema.Columns.Select(c => new
                {
                    t.Schema.TableName,
                    c.ColumnName
                }))
                .SelectMany(c => c)
                .FirstOrDefault(o => o.ColumnName.Contains("$"));
            var availableBlockTable = new TypedTable<AvailableBlockRecord>(
                this,
                TypedTableSchema<AvailableBlockRecord>.FromConstructor("$availableBlock")
                .AddPrimaryKeyProperty(a => a.BlockId));

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
            TombstoneTable = new TypedTable<TombstoneRecord>(
                this,
                TypedTableSchema<TombstoneRecord>.FromConstructor("$tombstone"));
            AvailabilityBlockManager = new AvailabilityBlockManager(
                availableBlockTable,
                _dbFileManager);
            QueryExecutionTable = new TypedTable<QueryExecutionRecord>(
                this,
                TypedTableSchema<QueryExecutionRecord>.FromConstructor("$queryExecution"));
            _dataLifeCycleManager = new DataLifeCycleManager(this);
            _schemaWithTriggers = userTables
                .Select(t => t.Schema)
                .Where(s => s.TriggerActions.Count > 0)
                .ToImmutableArray();
            _blobLock = blobLock;

            var tableMap = userTables
                .Select(t => new TableProperties(t, 1, null, false, true))
                .Append(new TableProperties(TombstoneTable, 1, null, true, false))
                .Append(new TableProperties(availableBlockTable, 1, null, true, false))
                .Append(new TableProperties(QueryExecutionTable, 1, null, true, true))
                .ToImmutableDictionary(t => t.Table.Schema.TableName);

            _databaseState = new DatabaseState(tableMap);
            DatabasePolicy = databasePolicies;
            DatabasePolicy.LogPolicy.StorageConfiguration?.Validate();
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

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ((IAsyncDisposable)_dataLifeCycleManager).DisposeAsync();
            if (_dbFileManager.IsValueCreated)
            {
                ((IDisposable)_dbFileManager.Value).Dispose();
            }
            if (_logFlushManager != null)
            {
                await ((IAsyncDisposable)_logFlushManager).DisposeAsync();
            }
            if (_blobLock != null)
            {
                await ((IAsyncDisposable)_blobLock).DisposeAsync();
            }
        }

        #region Public interface
        public DatabasePolicy DatabasePolicy { get; }

        internal AvailabilityBlockManager AvailabilityBlockManager { get; }

        /// <summary>
        /// For DEBUG purposes.
        /// Action executed before and after a transaction completion (in DEBUG only).
        /// </summary>
        internal Action TransactionAction = () => { };

        public Table GetTable(string tableName)
        {
            if (_databaseState.TableMap.TryGetValue(tableName, out var table))
            {
                if (table.IsUserTable)
                {
                    return table.Table;
                }
                else
                {
                    throw new InvalidOperationException($"Table '{tableName}' isn't a user table");
                }
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

        internal Table GetAnyTable(string tableName)
        {
            if (_databaseState.TableMap.TryGetValue(tableName, out var table))
            {
                return table.Table;
            }
            else
            {
                throw new InvalidOperationException($"Table '{tableName}' doesn't exist");
            }
        }

        /// <summary>
        /// Awaits life cycle management processing committed data up to <paramref name="tolerance"/>
        /// times the policies.
        /// More concretely, this method looks at <see cref="DatabasePolicy.InMemoryPolicy"/>.
        /// </summary>
        /// <param name="tolerance"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task AwaitLifeCycleManagementAsync(double tolerance, CancellationToken ct)
        {
            bool IsToleranceExceeded()
            {
                //  We use <= (lesser or equal) so that we don't spin
                //  when tolerance==1
                using (var tx = CreateTransaction())
                {
                    var state = GetDatabaseStateSnapshot();
                    var tableMap = state.TableMap;
                    var policy = DatabasePolicy.InMemoryPolicy;
                    var maxBlocksPerTable = tx.TransactionState
                        .InMemoryDatabase
                        .GetMaxInMemoryBlocksPerTable();

                    if (maxBlocksPerTable <= tolerance * policy.MaxBlocksPerTable)
                    {
                        var totalNonMetaDataRecords = tableMap.Values
                            .Where(p => p.IsPersisted)
                            .Where(p => !p.IsMetaDataTable)
                            .Select(p => p.Table.Query(tx).WithInMemoryOnly().Count())
                            .Sum();

                        if (totalNonMetaDataRecords <= tolerance * policy.MaxNonMetaDataRecords)
                        {
                            var totalMetaDataRecords = tableMap.Values
                                .Where(p => p.IsPersisted)
                                .Where(p => p.IsMetaDataTable)
                                .Select(p => p.Table.Query(tx).WithInMemoryOnly().Count())
                                .Sum();

                            if (totalMetaDataRecords <= tolerance * policy.MaxMetaDataRecords)
                            {
                                var totalTombstonedRecords = TombstoneTable.Query(tx).Count();

                                if (totalTombstonedRecords <= tolerance * policy.MaxTombstonedRecords)
                                {
                                    return false;
                                }
                            }
                        }
                    }

                    return true;
                }
            }

            while (IsToleranceExceeded())
            {
                await ForceDataManagementAsync();
            }
        }

        #region System tables
        public DatabaseStatistics GetDatabaseStatistics()
        {
            using (var tx = CreateTransaction())
            {
                return DatabaseStatistics.Create(this);
            }
        }

        public TypedTableQuery<QueryExecutionRecord> QueryQueryExecution(TransactionContext? tc = null)
        {
            return new TypedTableQuery<QueryExecutionRecord>(
                QueryExecutionTable,
                false,
                tc);
        }

        internal TypedTable<QueryExecutionRecord> QueryExecutionTable { get; }
        #endregion
        #endregion

        #region Meta data tables
        internal bool HasMetaDataTable(string tableName)
        {
            var existingMap = _databaseState.TableMap;

            if (existingMap.TryGetValue(tableName, out var table))
            {
                return table.MetadataTableName != null;
            }
            else
            {
                throw new InvalidOperationException($"Table '{tableName}' doesn't exist");
            }
        }

        /// <summary>Returns the corresponding metadata of a given table.</summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        internal Table GetMetaDataTable(string tableName)
        {
            var map = _databaseState.TableMap;

            if (map.TryGetValue(tableName, out var table))
            {
                if (!table.IsPersisted)
                {
                    throw new InvalidOperationException($"Table '{tableName}' can't be persisted");
                }
                if (table.MetadataTableName != null)
                {
                    if (map.TryGetValue(table.MetadataTableName, out var metaTable))
                    {
                        return metaTable.Table;
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            $"Metadata table '{table.MetadataTableName}' can't be found");
                    }
                }
                else
                {
                    var metaDataSchema = table.Table.Schema.CreateMetadataTableSchema();
                    var metaDataTable = new Table(this, metaDataSchema);

                    ChangeDatabaseState(state =>
                    {
                        if (state.TableMap.ContainsKey(metaDataSchema.TableName))
                        {   //  Meta data table was already created (on another thread)
                            return null;
                        }
                        else
                        {
                            var tableMap = state.TableMap.Add(
                                metaDataSchema.TableName,
                                new TableProperties(
                                    metaDataTable,
                                    table.Generation + 1,
                                    null,
                                    false,
                                    true))
                            .SetItem(tableName, state.TableMap[tableName] with
                            {
                                MetadataTableName = metaDataSchema.TableName
                            });
                            var newState = state with
                            {
                                TableMap = tableMap
                            };

                            return newState;
                        }
                    });

                    //  Re-run, now with the map containing the meta data table
                    return GetMetaDataTable(tableName);
                }
            }
            else
            {
                throw new InvalidOperationException($"Table '{tableName}' doesn't exist");
            }
        }
        #endregion

        #region State
        internal DatabaseState GetDatabaseStateSnapshot()
        {
            return _databaseState;
        }

        internal (DatabaseState OldState, DatabaseState? NewState) ChangeDatabaseState(
            Func<DatabaseState, DatabaseState?> stateChange)
        {   //  Optimistically try to change the db state:  repeat if necessary
            var oldState = _databaseState;
            var newState = stateChange(oldState);

            if (newState == null)
            {
                return (oldState, null);
            }
            else if (object.ReferenceEquals(
                oldState,
                Interlocked.CompareExchange(ref _databaseState, newState, oldState)))
            {
                return (oldState, newState);
            }
            else
            {   //  Exchange fail, we retry
                return ChangeDatabaseState(stateChange);
            }
        }
        #endregion

        #region Transaction
        internal bool HasActiveTransaction => _activeTransactionCount != 0;

        public TransactionContext CreateTransaction()
        {
            return CreateTransaction(true, new TransactionLog());
        }

        internal TransactionContext CreateTransaction(bool doLog, TransactionLog transactionLog)
        {
            _dataLifeCycleManager.ObserveBackgroundTask();
            IncrementActiveTransactionCount();

            var transactionContext = new TransactionContext(
                this,
                _databaseState.InMemoryDatabase,
                transactionLog,
                doLog);

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

        internal void CompleteTransaction(TransactionContext tx)
        {
            CompleteTransactionInternal(tx);
        }

        internal async Task CompleteTransactionAsync(TransactionContext tx, CancellationToken ct)
        {
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            CompleteTransactionInternal(tx, tcs, ct);
            if (!(tx.DoLog && _logFlushManager != null))
            {
                await ForceDataManagementAsync(DataManagementActivity.None);
                tcs.TrySetResult();
            }

            await tcs.Task;
        }

        private void CompleteTransactionInternal(
            TransactionContext tx,
            TaskCompletionSource? tcs = null,
            CancellationToken ct = default)
        {
            if (tx.DoLog)
            {
                RunTriggers(tx);
            }
            RunTransactionAction();
            CompleteTransactionState(tx.TransactionState, tx.DoLog, tcs, ct);
            RunTransactionAction();
            _dataLifeCycleManager.TriggerDataManagement();
            DecrementActiveTransactionCount();
        }

        [Conditional("DEBUG")]
        private void RunTransactionAction()
        {
            if (TransactionAction != null)
            {
                TransactionAction();
            }
        }

        private void CompleteTransactionState(
            TransactionState transactionState,
            bool doLog,
            TaskCompletionSource? tcs,
            CancellationToken ct)
        {
            var isTransactionLogged = doLog && _logFlushManager != null;
            var counts = isTransactionLogged
                ? transactionState.UncommittedTransactionLog.GetLoggedRecordCounts(
                    GetDatabaseStateSnapshot().TableMap
                    .Where(t => t.Value.IsUserTable)
                    .Select(t => t.Key),
                    TombstoneTable.Schema.TableName)
                : (0, 0);
            var states = ChangeDatabaseState(oldState =>
            {
                var newState = oldState with
                {
                    InMemoryDatabase = oldState.InMemoryDatabase.CommitLog(transactionState),
                    AppendRecordCount = oldState.AppendRecordCount + counts.AppendRecordCount,
                    TombstoneRecordCount = oldState.TombstoneRecordCount + counts.TombstoneRecordCount
                };

                if (isTransactionLogged)
                {
                    if (newState.AppendRecordCount >= DatabasePolicy.LogPolicy.MinRecordCountBeforeCheckpoint
                    && newState.TombstoneRecordCount * 100
                    > newState.AppendRecordCount * DatabasePolicy.LogPolicy.MinTombstonePercentBeforeCheckpoint)
                    {   //  Trigger checkpoint
                        newState = newState with
                        {
                            AppendRecordCount = 0,
                            TombstoneRecordCount = 0,
                            TransactionLogItems = new ReversedLinkedList<TransactionLogItem>(
                                new TransactionLogItem(
                                    null,
                                    () => ListCheckpointTransactions(newState.InMemoryDatabase),
                                    tcs,
                                    ct),
                                newState.TransactionLogItems)
                        };
                    }
                    else
                    {   //  Just logs
                        newState = newState with
                        {
                            TransactionLogItems = new ReversedLinkedList<TransactionLogItem>(
                                new TransactionLogItem(
                                    transactionState.UncommittedTransactionLog,
                                    null,
                                    tcs,
                                    ct),
                                newState.TransactionLogItems)
                        };
                    }
                }

                return newState;
            });

            if (states.NewState!.TransactionLogItems != null
                && states.NewState!.TransactionLogItems.Content.TransactionLogsFunc != null)
            {   //  We increment here and decrement at the end of ListCheckpointTransactions
                IncrementActiveTransactionCount();
            }
            if (doLog)
            {
                _logFlushManager?.Push();
            }
            ThrowOnPhantomTombstones(states.OldState, doLog);
        }

        private void ThrowOnPhantomTombstones(DatabaseState oldState, bool doLog)
        {
            if (doLog && DatabasePolicy.DiagnosticPolicy.ThrowOnPhantomTombstones)
            {
                Interlocked.Increment(ref _activeTransactionCount);

                //  Load a tx with old state to run validations
                using (var tx = new TransactionContext(
                    this,
                    oldState.InMemoryDatabase,
                    new TransactionLog(),
                    false))
                {
                    var tombstoneRecordsByTable = TombstoneTable.Query(tx)
                        .GroupBy(t => t.TableName);

                    foreach (var g in tombstoneRecordsByTable)
                    {
                        var tableName = g.Key;
                        var table = GetAnyTable(tableName);
                        var predicate = new InPredicate(
                            table.Schema.RecordIdColumnIndex,
                            g.Select(t => t.DeletedRecordId).Cast<object?>(),
                            true);
                        var foundRecordIds = table.Query(tx)
                            .WithIgnoreDeleted()
                            .WithPredicate(predicate)
                            .WithProjection(table.Schema.RecordIdColumnIndex)
                            .Select(r => (long)r.Span[0]!);
                        var phantomRecordIds = g.Select(t => t.DeletedRecordId).Except(foundRecordIds);
                        var phantomRecordCount = phantomRecordIds.Count();

                        if (phantomRecordCount > 0)
                        {
                            throw new InvalidOperationException(
                                $"Table {tableName} has {phantomRecordCount} phantom records");
                        }
                    }
                }
            }
        }

        private void IncrementActiveTransactionCount()
        {
            Interlocked.Increment(ref _activeTransactionCount);
        }

        private void DecrementActiveTransactionCount()
        {
            if (Interlocked.Decrement(ref _activeTransactionCount) < 0)
            {
                throw new InvalidOperationException("Transaction count is below zero");
            }
        }

        internal void RollbackTransaction()
        {
            if (Interlocked.Decrement(ref _activeTransactionCount) < 0)
            {
                throw new InvalidOperationException("Transaction count is below zero");
            }
        }

        internal async Task ForceDataManagementAsync(
            DataManagementActivity dataManagementActivity = DataManagementActivity.None)
        {
            await _dataLifeCycleManager.ForceDataManagementAsync(dataManagementActivity);
        }
        #endregion

        #region Triggers
        private void RunTriggers(TransactionContext tx)
        {
            bool IsTriggerRequired(
                string tableName,
                IDictionary<string, TransactionTableLog> transactionTableLogMap,
                IBlock? tombstoneBlock,
                QueryPredicateFactory<TombstoneRecord>? qpf)
            {
                if (transactionTableLogMap.TryGetValue(tableName, out var tableLog)
                    && ((IBlock)tableLog.NewDataBlock).RecordCount > 0)
                {
                    return true;
                }
                else if (tombstoneBlock != null && qpf != null)
                {
                    var filteredOuput = tombstoneBlock.Filter(
                        qpf.Equal(t => t.TableName, tableName).QueryPredicate,
                        false);

                    return filteredOuput.RowIndexes.Any();
                }
                else
                {
                    return false;
                }
            }

            if (_schemaWithTriggers.Count > 0)
            {
                var transactionTableLogMap =
                    tx.TransactionState.UncommittedTransactionLog.TransactionTableLogMap;
                var hasTombstoneLogs = transactionTableLogMap.TryGetValue(
                    TombstoneTable.Schema.TableName,
                    out var tombstoneLog);
                var tombstoneBlock = tombstoneLog?.NewDataBlock;
                var qpf = tombstoneBlock != null
                    ? new QueryPredicateFactory<TombstoneRecord>(
                        (TypedTableSchema<TombstoneRecord>)((IBlock)tombstoneBlock).TableSchema)
                    : null;

                foreach (var schema in _schemaWithTriggers)
                {
                    if (IsTriggerRequired(
                        schema.TableName,
                        transactionTableLogMap,
                        tombstoneBlock,
                        qpf))
                    {
                        foreach (var trigger in schema.TriggerActions)
                        {
                            trigger(_databaseContext!, tx);
                        }
                    }
                }
            }
        }
        #endregion

        #region Tombstone
        internal TypedTable<TombstoneRecord> TombstoneTable { get; }

        internal void DeleteRecord(
            long recordId,
            string tableName,
            TransactionContext tx)
        {
            TombstoneTable.AppendRecord(
                new TombstoneRecord(recordId, tableName, DateTime.Now),
                tx);
        }

        internal IEnumerable<long> GetDeletedRecordIds(
            string tableName,
            TransactionContext transactionContext)
        {
            return tableName != TombstoneTable.Schema.TableName
                ? TombstoneTable.Query(transactionContext)
                .Where(ts => ts.TableName == tableName)
                .Select(ts => ts.DeletedRecordId)
                : Array.Empty<long>();
        }

        internal void DeleteTombstoneRecords(
            string tableName,
            IEnumerable<long> recordIds,
            TransactionContext tx)
        {
            var tombstoneTableName = TombstoneTable.Schema.TableName;
            var materializedRecordIds = recordIds.Distinct().ToImmutableArray();

            tx.LoadCommittedBlocksInTransaction(tombstoneTableName);

            var transactionTableLog = tx.TransactionState
                .UncommittedTransactionLog
                .TransactionTableLogMap[tombstoneTableName];
            var committedDataBlock = transactionTableLog.CommittedDataBlock;
            var newDataBlock = transactionTableLog.NewDataBlock;
            var tombstonePredicate = TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.In(t => t.DeletedRecordId, materializedRecordIds))
                .Predicate;

            if (committedDataBlock != null && ((IBlock)committedDataBlock).RecordCount > 0)
            {
                var rowIndexes = ((IBlock)committedDataBlock)
                    .Filter(tombstonePredicate, false)
                    .RowIndexes;

                committedDataBlock.DeleteRecordsByRecordIndex(rowIndexes);
            }
            if (newDataBlock != null && ((IBlock)newDataBlock).RecordCount > 0)
            {
                var rowIndexes = ((IBlock)newDataBlock)
                    .Filter(tombstonePredicate, false)
                    .RowIndexes;

                newDataBlock.DeleteRecordsByRecordIndex(rowIndexes);
            }
        }
        #endregion

        #region Block load
        internal IBlock GetOrLoadBlock(int blockId, TableSchema schema)
        {
            var payload = _dbFileManager.Value.ReadBlock(blockId);
            var block = ReadOnlyBlock.Load(payload, schema);

            return block;
        }

        internal void PersistBlock(int blockId, ReadOnlySpan<byte> buffer, TransactionContext tx)
        {
            _dbFileManager.Value.WriteBlock(blockId, buffer);
        }
        #endregion

        #region Logging
        private IEnumerable<TransactionLogItem> FlushTransactionLogItems()
        {
            var states = ChangeDatabaseState(oldState =>
            {
                if (oldState.TransactionLogItems != null)
                {
                    return oldState with
                    {
                        TransactionLogItems = null
                    };
                }
                else
                {
                    return null;
                }
            });

            if (states.NewState != null && states.OldState.TransactionLogItems != null)
            {
                return states.OldState.TransactionLogItems.ToEnumerable();
            }
            else
            {
                return Array.Empty<TransactionLogItem>();
            }
        }

        private async Task InitLogsAsync(BlobClients blobClients, CancellationToken ct)
        {
            var userTableSchemas = GetDatabaseStateSnapshot().TableMap.Values
                .Where(p => p.IsUserTable)
                .Select(p => p.Table.Schema);
            var logTransactionReader = await LogTransactionReader.CreateAsync(
                DatabasePolicy.LogPolicy,
                Path.Combine(Path.GetTempPath(), "track-db", Guid.NewGuid().ToString()),
                blobClients,
                userTableSchemas,
                TombstoneTable,
                ct);
            var tableToLastRecordIdMap = new Dictionary<string, long>();
            var tombstoneTableName = TombstoneTable.Schema.TableName;
            var loggedTableNames = userTableSchemas
                .Select(t => t.TableName);
            (var appendRecordCount, var tombstoneRecordCount) = await ProcessLoggedTransactionsAsync(
                logTransactionReader,
                tableToLastRecordIdMap,
                tombstoneTableName,
                loggedTableNames,
                ct);

            //  Set the current record ID for each table
            foreach (var pair in tableToLastRecordIdMap)
            {
                var tableName = pair.Key;
                var maxRecordId = pair.Value;

                GetAnyTable(tableName).InitRecordId(maxRecordId);
            }
            ChangeDatabaseState(state => state with
            {
                AppendRecordCount = appendRecordCount,
                TombstoneRecordCount = tombstoneRecordCount
            });

            var logTransactionWriter =
                await logTransactionReader.CreateLogTransactionWriterAsync(ct);

            _logFlushManager = new LogFlushManager(FlushTransactionLogItems, logTransactionWriter);
        }

        private async Task<(long AppendRecordCount, long TombstoneRecordCount)> ProcessLoggedTransactionsAsync(
            LogTransactionReader logTransactionReader,
            Dictionary<string, long> tableToLastRecordIdMap,
            string tombstoneTableName,
            IEnumerable<string> loggedTableNames,
            CancellationToken ct)
        {
            long appendRecordCount = 0;
            long tombstoneRecordCount = 0;
            var i = 0;

            await foreach (var transactionLog in logTransactionReader.LoadTransactionsAsync(ct))
            {
                var counts = transactionLog.GetLoggedRecordCounts(
                    loggedTableNames,
                    tombstoneTableName);

                transactionLog.UpdateLastRecordIdMap(tableToLastRecordIdMap);
                appendRecordCount += counts.AppendRecordCount;
                tombstoneRecordCount += counts.TombstoneRecordCount;
                using (var tx = CreateTransaction(false, transactionLog))
                {
                    tx.Complete();
                }
                if (++i % 10 == 0)
                {
                    await AwaitLifeCycleManagementAsync(2, ct);
                }
            }

            return (appendRecordCount, tombstoneRecordCount);
        }

        private IEnumerable<TransactionLog> ListCheckpointTransactions(
            InMemoryDatabase inMemoryDatabase)
        {
            using (var tx = new TransactionContext(this, inMemoryDatabase, new(), false))
            {
                var userTables = GetDatabaseStateSnapshot().TableMap
                    .Where(t => t.Value.IsUserTable)
                    .Select(t => t.Value.Table);
                var recordsPerTransaction = DatabasePolicy.InMemoryPolicy.MaxNonMetaDataRecords;

                foreach (var table in userTables)
                {
                    var records = table.Query(tx)
                        //  Add hidden columns
                        .WithProjection(Enumerable.Range(0, table.Schema.ColumnProperties.Count))
                        .AsEnumerable();
                    var recordEnumerator = records.GetEnumerator();
                    var doContinue = true;

                    while (doContinue)
                    {
                        var txLog = new TransactionLog();
                        var recordCount = 0;

                        for (var i = 0; i != recordsPerTransaction && recordEnumerator.MoveNext(); ++i)
                        {
                            var record = recordEnumerator.Current;
                            var recordId = (long)record.Span[table.Schema.RecordIdColumnIndex]!;
                            var trimmedRecord = record.Span.Slice(0, table.Schema.Columns.Count);

                            txLog.AppendRecord(recordId, trimmedRecord, table.Schema);
                            ++recordCount;
                        }
                        yield return txLog;
                        doContinue = txLog.TransactionTableLogMap.Any()
                            && recordCount == recordsPerTransaction;
                    }
                }

                //  At this point we decrement the active transaction count that was activated outside
                tx.Complete();
            }
        }
        #endregion
    }
}