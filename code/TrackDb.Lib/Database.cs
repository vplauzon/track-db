using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.DataLifeCycle;
using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.Logging;
using TrackDb.Lib.Policies;
using TrackDb.Lib.Predicate;
using TrackDb.Lib.SystemData;

namespace TrackDb.Lib
{
    /// <summary>
    /// Database:  a collection of tables that can share transactions
    /// and are persisted in the same file.
    /// </summary>
    public class Database : IAsyncDisposable
    {
        private readonly Lazy<StorageManager> _storageManager;
        private readonly LogManager? _logManager;
        private readonly TypedTable<TombstoneRecord> _tombstoneTable;
        private readonly TypedTable<AvailableBlockRecord> _availableBlockTable;
        private readonly DataLifeCycleManager _dataLifeCycleManager;
        private long _recordId = 0;
        private volatile DatabaseState _databaseState;

        #region Constructors
        public async static Task<Database> CreateAsync(
            DatabasePolicy databasePolicies,
            params IEnumerable<TableSchema> schemas)
        {
            var database = new Database(databasePolicies, schemas);

            await database.InitLogsAsync();

            return database;
        }

        private Database(DatabasePolicy databasePolicies, params IEnumerable<TableSchema> schemas)
        {
            var userTables = schemas
                .Select(s => CreateTable(s))
                .ToImmutableArray();

            _storageManager = new Lazy<StorageManager>(
                () => new StorageManager(Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.db")),
                LazyThreadSafetyMode.ExecutionAndPublication);
            _logManager = databasePolicies.LogPolicy.StorageConfiguration != null
                ? new LogManager(databasePolicies.LogPolicy)
                : null;

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
            _tombstoneTable = new TypedTable<TombstoneRecord>(
                this,
                TypedTableSchema<TombstoneRecord>.FromConstructor("$tombstone"));
            _availableBlockTable = new TypedTable<AvailableBlockRecord>(
                this,
                TypedTableSchema<AvailableBlockRecord>.FromConstructor("$availableBlock"));
            QueryExecutionTable = new TypedTable<QueryExecutionRecord>(
                this,
                TypedTableSchema<QueryExecutionRecord>.FromConstructor("$queryExecution"));
            _dataLifeCycleManager = new DataLifeCycleManager(this, _tombstoneTable, _storageManager);

            var tableMap = userTables
                .Select(t => new TableProperties(t, null, true, false, false))
                .Append(new TableProperties(_tombstoneTable, null, false, false, true))
                .Append(new TableProperties(_availableBlockTable, null, false, false, false))
                .Append(new TableProperties(QueryExecutionTable, null, false, false, false))
                .ToImmutableDictionary(t => t.Table.Schema.TableName);

            _databaseState = new DatabaseState(tableMap);
            DatabasePolicy = databasePolicies;
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
            await ValueTask.CompletedTask;
            if (_storageManager.IsValueCreated)
            {
                ((IDisposable)_storageManager.Value).Dispose();
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

        #region System tables
        public DatabaseStatistics GetDatabaseStatistics()
        {
            var state = _databaseState;
            var inMemoryDatabase = state.InMemoryDatabase;
            var userTableNames = state.TableMap
                .Where(p => p.Value.IsUserTable)
                .Select(p => p.Key)
                .ToImmutableHashSet();
            var userTableRecordCount = inMemoryDatabase.TableTransactionLogsMap
                .Where(p => userTableNames.Contains(p.Key))
                .SelectMany(p => p.Value.InMemoryBlocks)
                .Sum(block => block.RecordCount);
            var inMemoryTombstoneRecords = 0;

            if (inMemoryDatabase.TableTransactionLogsMap.ContainsKey(
                _tombstoneTable.Schema.TableName))
            {
                inMemoryTombstoneRecords +=
                    inMemoryDatabase.TableTransactionLogsMap[_tombstoneTable.Schema.TableName]
                    .InMemoryBlocks
                    .Sum(block => block.RecordCount);
            }

            return new DatabaseStatistics(userTableRecordCount, inMemoryTombstoneRecords);
        }

        public TypedTableQuery<QueryExecutionRecord> QueryQueryExecution(TransactionContext? tc = null)
        {
            return new TypedTableQuery<QueryExecutionRecord>(
                QueryExecutionTable,
                false,
                tc);
        }

        internal int GetFreeBlockId()
        {
            var availableBlock = _availableBlockTable.Query()
                .Take(1)
                .FirstOrDefault();

            if (availableBlock != null)
            {
                _availableBlockTable.Query()
                    .Where(pf => pf.Equal(b => b.BlockId, availableBlock.BlockId))
                    .Delete();

                return availableBlock.BlockId;
            }
            else
            {
                var blockIds = _storageManager.Value.CreateBlockBatch()
                    .ToImmutableArray();

                _availableBlockTable.AppendRecords(blockIds
                    .Skip(1)
                    .Select(id => new AvailableBlockRecord(id)));

                return blockIds.First();
            }
        }

        internal void ReleaseBlockIds(IEnumerable<int> blockIds)
        {
            _availableBlockTable.AppendRecords(blockIds
                .Select(id => new AvailableBlockRecord(id)));
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
            var existingMap = _databaseState.TableMap;

            if (existingMap.TryGetValue(tableName, out var table))
            {
                if (!table.IsPersisted)
                {
                    throw new InvalidOperationException($"Table '{tableName}' can't be persisted");
                }
                if (table.MetaDataTableName != null)
                {
                    if (existingMap.TryGetValue(table.MetaDataTableName, out var metaTable))
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
                    var metaDataTableName = GetMetadataTableName(tableName);
                    var metaDataSchema =
                        CreateMetaDataSchema(metaDataTableName, table.Table.Schema);
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
                                metaDataTableName,
                                new TableProperties(metaDataTable, null, false, true, false))
                            .SetItem(tableName, state.TableMap[tableName] with
                            {
                                MetaDataTableName = metaDataTableName
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
                    new ColumnSchema(MetadataColumns.ITEM_COUNT, typeof(int)),
                    new ColumnSchema(MetadataColumns.BLOCK_ID, typeof(int))
                })
                //  We fan out the columns
                .SelectMany(c => c);
            var metaDataSchema = new TableSchema(
                metaDataTableName,
                metaDataColumns,
                Array.Empty<int>(),
                Array.Empty<int>());

            return metaDataSchema;
        }
        #endregion

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
        public TransactionContext CreateTransaction()
        {
            _dataLifeCycleManager.ObserveBackgroundTask();

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
                },
                true);

            ChangeDatabaseState(currentDbState =>
            {
                var newTransactionMap = currentDbState.TransactionMap.Add(
                    transactionContext.TransactionId,
                    new TransactionState(currentDbState.InMemoryDatabase));

                return currentDbState with { TransactionMap = newTransactionMap };
            });

            return transactionContext;
        }

        internal TransactionContext CreateDummyTransaction()
        {
            var state = _databaseState;
            var transactionContext = new TransactionContext(
                this,
                new TransactionState(state.InMemoryDatabase));

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

        internal void CompleteTransaction(long transactionId, bool doLog)
        {
            //  Fetch transaction state
            var transactionState = _databaseState.TransactionMap[transactionId];

            ChangeDatabaseState(currentDbState =>
            {   //  Remove it from map
                var newTransactionMap = currentDbState.TransactionMap.Remove(transactionId);

                if (transactionState.UncommittedTransactionLog.IsEmpty)
                {
                    return currentDbState with { TransactionMap = newTransactionMap };
                }
                else
                {
                    return currentDbState with
                    {
                        InMemoryDatabase = currentDbState.InMemoryDatabase.CommitLog(
                            transactionState.UncommittedTransactionLog),
                        TransactionMap = newTransactionMap
                    };
                }
            });
            _dataLifeCycleManager.TriggerDataManagement();
            if (doLog && _logManager != null)
            {
                var contentText = ToTransactionContent(transactionState.UncommittedTransactionLog);

                if (contentText != null)
                {
                    _logManager.QueueContent(contentText);
                }
            }
        }

        internal void RollbackTransaction(long transactionId)
        {
            ChangeDatabaseState(currentDbState =>
            {   //  Remove transaction from map (and forget about it)
                var newTransactionMap = currentDbState.TransactionMap.Remove(transactionId);

                return currentDbState with { TransactionMap = newTransactionMap };
            });
        }

        internal async Task ForceDataManagementAsync(
            DataManagementActivity dataManagementActivity = DataManagementActivity.None)
        {
            await _dataLifeCycleManager.ForceDataManagementAsync(dataManagementActivity);
        }
        #endregion

        #region Tombstone
        internal void DeleteRecord(
            long recordId,
            int? blockId,
            string tableName,
            TransactionContext tc)
        {
            _tombstoneTable.AppendRecord(
                new TombstoneRecord(recordId, blockId, tableName, DateTime.Now),
                tc);
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

        #region Logging
        private async Task InitLogsAsync()
        {
            if (_logManager != null)
            {
                await _logManager.InitLogsAsync();
            }
        }

        private string? ToTransactionContent(TransactionLog transactionLog)
        {
            Dictionary<string, List<long>> ToTombstones(IBlock block)
            {
                var tableMap = _databaseState.TableMap;
                var userTableNames = tableMap
                    .Where(t => t.Value.IsUserTable)
                    .Select(t => t.Key);
                var pf = new QueryPredicateFactory<TombstoneRecord>(_tombstoneTable.Schema);
                var predicate = pf
                    .In(t => t.TableName, userTableNames)
                    .QueryPredicate;
                var rowIndexes = block.Filter(predicate, false).RowIndexes;
                var columnIndexes = _tombstoneTable.Schema.GetColumnIndexSubset(t => t.TableName)
                    .Concat(_tombstoneTable.Schema.GetColumnIndexSubset(t => t.RecordId));
                var buffer = new object[2];
                var content = block.Project(buffer, columnIndexes, rowIndexes, 0)
                    .Select(data => new
                    {
                        Table = (string)data.Span[0]!,
                        RecordId = (long)data.Span[1]!
                    })
                    .GroupBy(o => o.Table)
                    .ToDictionary(g => g.Key, g => g.Select(i => i.RecordId).ToList());

                return content;
            }

            var tableMap = _databaseState.TableMap;
            var tables = transactionLog.TableBlockBuilderMap
                .Where(p => tableMap[p.Key].IsUserTable)
                .Select(p => KeyValuePair.Create(p.Key, p.Value.ToLog()))
                .ToDictionary();
            var tombstones = transactionLog.TableBlockBuilderMap.ContainsKey(
                _tombstoneTable.Schema.TableName)
                ? ToTombstones(
                    transactionLog.TableBlockBuilderMap[_tombstoneTable.Schema.TableName])
                : new Dictionary<string, List<long>>();

            if (tables.Any() || tombstones.Any())
            {
                var content = new TransactionContent(tables, tombstones);
                var contentText = JsonSerializer.Serialize(content);

                return contentText;
            }
            else
            {
                return null;
            }
        }
        #endregion
    }
}