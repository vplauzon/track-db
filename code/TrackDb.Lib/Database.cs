using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Logging;
using TrackDb.Lib.Policies;
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
        const int CHECKPOINT_TX_RECORD_COUNT = 10;

        private readonly Lazy<DatabaseFileManager> _dbFileManager;
        private readonly TypedTable<AvailableBlockRecord> _availableBlockTable;
        private readonly DataLifeCycleManager _dataLifeCycleManager;
        private LogTransactionWriter? _logTransactionWriter;
        private volatile DatabaseState _databaseState;
        private volatile int _activeTransactionCount = 0;

        #region Constructors
        public async static Task<Database> CreateAsync(
            DatabasePolicy databasePolicies,
            params IEnumerable<TableSchema> schemas)
        {
            var database = new Database(databasePolicies, schemas);

            if (databasePolicies.LogPolicy.StorageConfiguration != null)
            {
                await database.InitLogsAsync(CancellationToken.None);
            }

            return database;
        }

        private Database(DatabasePolicy databasePolicies, params IEnumerable<TableSchema> schemas)
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
            _availableBlockTable = new TypedTable<AvailableBlockRecord>(
                this,
                TypedTableSchema<AvailableBlockRecord>.FromConstructor("$availableBlock")
                .AddPrimaryKeyProperty(a => a.BlockId));
            QueryExecutionTable = new TypedTable<QueryExecutionRecord>(
                this,
                TypedTableSchema<QueryExecutionRecord>.FromConstructor("$queryExecution"));
            _dataLifeCycleManager = new DataLifeCycleManager(this);

            var tableMap = userTables
                .Select(t => new TableProperties(t, 1, null, false, true))
                .Append(new TableProperties(TombstoneTable, 1, null, true, false))
                .Append(new TableProperties(_availableBlockTable, 1, null, true, true))
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
                await ((IAsyncDisposable)_dbFileManager.Value).DisposeAsync();
            }
            if (_logTransactionWriter != null)
            {
                await ((IAsyncDisposable)_logTransactionWriter).DisposeAsync();
            }
        }

        #region Public interface
        public DatabasePolicy DatabasePolicy { get; }

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
        /// <returns></returns>
        public async Task AwaitLifeCycleManagement(double tolerance)
        {
            bool IsToleranceExceeded()
            {
                using (var tx = CreateTransaction())
                {
                    var state = GetDatabaseStateSnapshot();
                    var tableMap = state.TableMap;
                    var policy = DatabasePolicy.InMemoryPolicy;
                    var maxBlocksPerTable = tx.TransactionState
                        .InMemoryDatabase
                        .GetMaxInMemoryBlocksPerTable();

                    if (maxBlocksPerTable < tolerance * policy.MaxBlocksPerTable)
                    {
                        var totalNonMetaDataRecords = tableMap.Values
                            .Where(p => p.IsPersisted)
                            .Where(p => !p.IsMetaDataTable)
                            .Select(p => p.Table.Query(tx).WithInMemoryOnly().Count())
                            .Sum();

                        if (totalNonMetaDataRecords < tolerance * policy.MaxNonMetaDataRecords)
                        {
                            var totalMetaDataRecords = tableMap.Values
                                .Where(p => p.IsPersisted)
                                .Where(p => p.IsMetaDataTable)
                                .Select(p => p.Table.Query(tx).WithInMemoryOnly().Count())
                                .Sum();

                            if (totalMetaDataRecords <= tolerance * policy.MaxMetaDataRecords)
                            {
                                var totalTombstonedRecords = TombstoneTable.Query(tx).Count();

                                if (totalTombstonedRecords < tolerance * policy.MaxTombstonedRecords)
                                {
                                    return false;
                                }
                            }
                        }
                    }

                    return true;
                }
            }
            if (tolerance <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(tolerance));
            }
            while (IsToleranceExceeded())
            {
                await Task.Delay(DatabasePolicy.LifeCyclePolicy.MaxWaitPeriod / 4);
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

        #region Available Blocks
        internal IReadOnlyList<int> GetAvailableBlockIds(int blockIdCount, TransactionContext tx)
        {
            var availableBlockIds = _availableBlockTable.Query(tx)
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.Available))
                .Take(blockIdCount)
                .Select(a => a.BlockId)
                .ToImmutableArray();

            if (availableBlockIds.Length == blockIdCount)
            {
                var newRecords = availableBlockIds
                    .Select(id => new AvailableBlockRecord(id, BlockAvailability.InUsed));

                _availableBlockTable.Query(tx)
                    .Where(pf => pf.In(a => a.BlockId, availableBlockIds))
                    .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.Available))
                    .Delete();
                _availableBlockTable.AppendRecords(newRecords, tx);

                return availableBlockIds;
            }
            else
            {
                var blockIds = _dbFileManager.Value.CreateBlockBatch()
                    .ToImmutableArray();

                _availableBlockTable.AppendRecords(blockIds
                    .Select(id => new AvailableBlockRecord(id, BlockAvailability.Available)),
                    tx);

                //  Now that there are available block, let's try again
                return GetAvailableBlockIds(blockIdCount, tx);
            }
        }

        internal int GetAvailableBlockId(TransactionContext tx)
        {
            var blockIds = GetAvailableBlockIds(1, tx);

            return blockIds[0];
        }

        internal void SetNoLongerInUsedBlockIds(IEnumerable<int> blockIds, TransactionContext tx)
        {
            var deletedUsedBlocks = _availableBlockTable.Query(tx)
                .Where(pf => pf.In(a => a.BlockId, blockIds))
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.InUsed))
                .Delete();

            if (deletedUsedBlocks != blockIds.Count())
            {
                throw new InvalidOperationException(
                    $"Corrupted available blocks:  {blockIds.Count()} to release from use, " +
                    $"{deletedUsedBlocks} found");
            }
            _availableBlockTable.AppendRecords(
                blockIds
                .Select(id => new AvailableBlockRecord(id, BlockAvailability.NoLongerInUsed)),
                tx);
        }

        internal bool ReleaseNoLongerInUsedBlocks(TransactionContext tx)
        {
            var noLongerInUsedBlocks = _availableBlockTable.Query(tx)
                .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.NoLongerInUsed))
                .ToImmutableArray();

            if (noLongerInUsedBlocks.Any())
            {
                _availableBlockTable.Query(tx)
                    .Where(pf => pf.Equal(a => a.BlockAvailability, BlockAvailability.NoLongerInUsed))
                    .Delete();
                _availableBlockTable.AppendRecords(noLongerInUsedBlocks
                    .Select(b => new AvailableBlockRecord(b.BlockId, BlockAvailability.Available)),
                    tx);

                return true;
            }
            else
            {
                return false;
            }
        }
        #endregion
        #endregion
        #endregion

        #region Meta data tables
        internal bool HasMetaDataTable(string tableName)
        {
            var existingMap = _databaseState.TableMap;

            if (existingMap.TryGetValue(tableName, out var table))
            {
                return table.MetaDataTableName != null;
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
                if (table.MetaDataTableName != null)
                {
                    if (map.TryGetValue(table.MetaDataTableName, out var metaTable))
                    {
                        return metaTable.Table;
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            $"Metadata table '{table.MetaDataTableName}' can't be found");
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
                                MetaDataTableName = metaDataSchema.TableName
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

        internal DatabaseState ChangeDatabaseState(Func<DatabaseState, DatabaseState?> stateChange)
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

        #region Transaction
        internal bool HasActiveTransaction => _activeTransactionCount != 0;

        public TransactionContext CreateTransaction()
        {
            return CreateTransaction(true, new TransactionLog());
        }

        internal TransactionContext CreateTransaction(bool doLog, TransactionLog transactionLog)
        {
            _dataLifeCycleManager.ObserveBackgroundTask();

            var transactionContext = new TransactionContext(
                this,
                _databaseState.InMemoryDatabase,
                transactionLog,
                doLog);

            Interlocked.Increment(ref _activeTransactionCount);

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

        internal void CompleteTransaction(TransactionState transactionState, bool doLog)
        {
            CompleteTransaction(transactionState);
            if (doLog && _logTransactionWriter != null)
            {
                _logTransactionWriter.QueueContent(transactionState.UncommittedTransactionLog);
            }
        }

        internal async Task LogAndCompleteTransactionAsync(TransactionState transactionState, bool doLog)
        {
            if (doLog && _logTransactionWriter != null)
            {
                await _logTransactionWriter.CommitContentAsync(
                    transactionState.UncommittedTransactionLog);
            }
            CompleteTransaction(transactionState);
        }

        private void CompleteTransaction(TransactionState transactionState)
        {
            if (!transactionState.UncommittedTransactionLog.IsEmpty)
            {
                if (_logTransactionWriter != null)
                {
                    var counts = transactionState.UncommittedTransactionLog.GetLoggedRecordCounts(
                        GetDatabaseStateSnapshot().TableMap
                        .Where(t => t.Value.IsUserTable)
                        .Select(t => t.Key),
                        TombstoneTable.Schema.TableName);

                    ChangeDatabaseState(currentDbState =>
                    {
                        return currentDbState with
                        {
                            InMemoryDatabase = currentDbState.InMemoryDatabase.CommitLog(transactionState),
                            AppendRecordCount = currentDbState.AppendRecordCount + counts.AppendRecordCount,
                            TombstoneRecordCount = currentDbState.TombstoneRecordCount + counts.TombstoneRecordCount
                        };
                    });
                    CheckpointIfNeeded();
                }
                else
                {
                    ChangeDatabaseState(currentDbState =>
                    {
                        return currentDbState with
                        {
                            InMemoryDatabase = currentDbState.InMemoryDatabase.CommitLog(transactionState)
                        };
                    });
                }
                _dataLifeCycleManager.TriggerDataManagement();
            }
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

        #region Tombstone
        internal TypedTable<TombstoneRecord> TombstoneTable { get; }

        internal void DeleteRecord(
            long recordId,
            int? blockId,
            string tableName,
            TransactionContext tx)
        {
            TombstoneTable.AppendRecord(
                new TombstoneRecord(recordId, tableName, blockId, DateTime.Now),
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

            tx.LoadCommittedBlocksInTransaction(tombstoneTableName);

            var transactionTableLog = tx.TransactionState
                .UncommittedTransactionLog
                .TransactionTableLogMap[tombstoneTableName];
            var newBlockBuilder = transactionTableLog.NewDataBlock;
            var committedBlockBuilder = transactionTableLog.CommittedDataBlock;
            var predicate = TombstoneTable.Query(tx)
                .Where(pf => pf.Equal(t => t.TableName, tableName))
                .Where(pf => pf.In(t => t.DeletedRecordId, recordIds))
                .Predicate;

            if (newBlockBuilder != null && ((IBlock)newBlockBuilder).RecordCount > 0)
            {
                var rowIndexes = ((IBlock)newBlockBuilder)
                    .Filter(predicate, false)
                    .RowIndexes;

                newBlockBuilder.DeleteRecordsByRecordIndex(rowIndexes);
            }
            if (committedBlockBuilder != null && ((IBlock)committedBlockBuilder).RecordCount > 0)
            {
                var rowIndexes = ((IBlock)committedBlockBuilder)
                    .Filter(predicate, false)
                    .RowIndexes;

                committedBlockBuilder.DeleteRecordsByRecordIndex(rowIndexes);
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
        private async Task InitLogsAsync(CancellationToken ct)
        {
            var tableMap = GetDatabaseStateSnapshot().TableMap;
            var logTransactionReader = await LogTransactionReader.CreateAsync(
                DatabasePolicy.LogPolicy,
                Path.Combine(Path.GetTempPath(), "track-db", Guid.NewGuid().ToString()),
                tableMap.Values
                .Where(p => p.IsUserTable)
                .Select(p => p.Table.Schema),
                TombstoneTable,
                ct);
            var tableToLastRecordIdMap = new Dictionary<string, long>();
            var tombstoneTableName = TombstoneTable.Schema.TableName;
            var loggedTableNames = GetDatabaseStateSnapshot().TableMap
                .Where(t => t.Value.IsUserTable)
                .Select(t => t.Key);
            long appendRecordCount = 0;
            long tombstonedCount = 0;

            await foreach (var transactionLog in logTransactionReader.LoadTransactionsAsync(ct))
            {
                var counts = transactionLog.GetLoggedRecordCounts(
                    loggedTableNames,
                    tombstoneTableName);

                appendRecordCount += counts.AppendRecordCount;
                tombstonedCount += counts.TombstoneRecordCount;
                using (var tx = CreateTransaction(false, transactionLog))
                {
                    tx.Complete();
                }
            }
            //  Set the current record ID for each table
            foreach (var pair in tableToLastRecordIdMap)
            {
                var tableName = pair.Key;
                var maxRecordId = pair.Value;

                tableMap[tableName].Table.InitRecordId(maxRecordId);
            }

            ChangeDatabaseState(state => state with
            {
                AppendRecordCount = appendRecordCount,
                TombstoneRecordCount = tombstonedCount
            });
            _logTransactionWriter = await logTransactionReader.CreateLogTransactionWriterAsync(ct);
        }

        private void CheckpointIfNeeded()
        {
            if (IsCheckpointNeeded())
            {
                throw new NotImplementedException();
            }
        }

        private bool IsCheckpointNeeded()
        {
            var state = GetDatabaseStateSnapshot();

            return state.AppendRecordCount > DatabasePolicy.LogPolicy.MinRecordCountBeforeCheckpoint
                && state.TombstoneRecordCount * DatabasePolicy.LogPolicy.MinRecordCountPerCheckpointTransaction
                > state.AppendRecordCount;
        }

        private async IAsyncEnumerable<TransactionLog> ListCheckpointTransactions(
            TransactionContext tx,
            [EnumeratorCancellation]
            CancellationToken ct)
        {
            var userTables = _databaseState.TableMap
                .Where(t => t.Value.IsUserTable)
                .Select(t => t.Value.Table);

            await ValueTask.CompletedTask;
            foreach (var table in userTables)
            {
                var records = table.Query()
                    //  Add record ID
                    .WithProjection(Enumerable.Range(0, table.Schema.Columns.Count + 1))
                    .AsEnumerable();
                var recordEnumerator = records.GetEnumerator();
                var doContinue = true;

                ct.ThrowIfCancellationRequested();
                while (doContinue)
                {
                    var txLog = new TransactionLog();

                    for (var i = 0; i != CHECKPOINT_TX_RECORD_COUNT && recordEnumerator.MoveNext(); ++i)
                    {
                        var record = recordEnumerator.Current;
                        var creationTime = (DateTime)record.Span[table.Schema.CreationTimeColumnIndex]!;
                        var recordId = (long)record.Span[table.Schema.RecordIdColumnIndex]!;
                        var dataRecord = record.Span.Slice(record.Length - 1);

                        txLog.AppendRecord(creationTime, recordId, dataRecord, table.Schema);
                    }
                    yield return txLog;
                    doContinue = txLog.TransactionTableLogMap.Any()
                        && ((IBlock)txLog.TransactionTableLogMap.First().Value).RecordCount == CHECKPOINT_TX_RECORD_COUNT;
                }
            }
        }
        #endregion
    }
}