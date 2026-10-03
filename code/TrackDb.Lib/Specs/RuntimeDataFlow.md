# Runtime Data Flow

1. A transaction writes appends and tombstones to its uncommitted transaction log.
2. On commit, TrackDb merges that log into the in-memory database and can flush it to optional
   Azure-backed transaction logging.
3. The lifecycle manager asynchronously merges logs, persists records into blocks, and hard
   deletes tombstoned records according to `DatabasePolicy` thresholds.
4. A query works from its transaction snapshot, filters candidate blocks using metadata and
   block statistics, loads blocks through `BlockCacheManager`, then projects and returns rows.

## Commit Durability

`Complete` makes a transaction consistent in memory and schedules persistence asynchronously. It
does not wait for local blocks or Azure transaction logs to be flushed.

`CompleteAsync` waits for the transaction and all earlier transactions to be flushed to logs. When
Azure Storage logging is configured, use it when the caller requires durable transaction history
before continuing.

```text
Database -> TransactionContext -> InMemory\TransactionLog
		 -> DataLifeCycle\DataLifeCycleManager -> persisted blocks / Logging
TableQuery -> metadata blocks -> BlockCacheManager -> InMemory\Block
```

The database creates internal `$tombstone` and `$queryExecution` tables. User table and column
names may not contain `$`; do not treat the internal tables as part of the public table API.