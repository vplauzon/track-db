using TrackDb.Lib.InMemory;
using TrackDb.Lib.InMemory.Block;
using TrackDb.Lib.DbStorage;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TrackDb.Lib.Policies;
using TrackDb.Lib.DataLifeCycle;
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
        private readonly TypedTable<TombstoneRecord> _tombstoneTable;
        private readonly TypedTable<QueryExecutionRecord> _queryExecutionTable;
        private readonly DataLifeCycleManager _dataLifeCycleManager;
        private long _recordId = 0;
        private volatile DatabaseState _databaseState;

        #region Constructors
        public async static Task<Database> CreateAsync(
            DatabasePolicies databasePolicies,
            params IEnumerable<TableSchema> schemas)
        {
            await Task.CompletedTask;

            return new Database(databasePolicies, schemas);
        }

        private Database(DatabasePolicies databasePolicies, params IEnumerable<TableSchema> schemas)
        {
            var userTables = schemas
                .Select(s => CreateTable(s))
                .ToImmutableArray();

            _storageManager = new Lazy<StorageManager>(
                () => new StorageManager(Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid()}.db")),
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
            _tombstoneTable = new TypedTable<TombstoneRecord>(
                this,
                TypedTableSchema<TombstoneRecord>.FromConstructor("$tombstone"));
            _queryExecutionTable = new TypedTable<QueryExecutionRecord>(
                this,
                TypedTableSchema<QueryExecutionRecord>.FromConstructor("$queryExecution"));
            _dataLifeCycleManager = new DataLifeCycleManager(this, _tombstoneTable, _storageManager);

            var tableMap = userTables
                .Select(t => new TableProperties(t, null, true, false, false))
                .Append(new TableProperties(_tombstoneTable, null, false, false, true))
                .Append(new TableProperties(_queryExecutionTable, null, false, false, false))
                .ToImmutableDictionary(t => t.Table.Schema.TableName);

            _databaseState = new DatabaseState(tableMap);
            DatabasePolicies = databasePolicies;
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

        #region Public interface
        public DatabasePolicies DatabasePolicies { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await ValueTask.CompletedTask;
            if (_storageManager.IsValueCreated)
            {
                ((IDisposable)_storageManager.Value).Dispose();
            }
        }

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
                });

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

        internal void CompleteTransaction(long transactionId)
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
    }
}