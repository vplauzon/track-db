# Core Model

`Database.CreateAsync` is the root entry point. Supply a `DatabasePolicy`, a
`DatabaseContextBase` factory, a cancellation token, and one or more table schemas. A context
usually exposes typed table properties obtained with `Database.GetTypedTable<T>`.

`TypedTableSchema<T>.FromConstructor` maps a record type's constructor parameters to columns.
`TypedTable<T>` provides typed append, query, delete, and update APIs; `Table` and `TableQuery`
provide their untyped equivalents.

Primary keys are optional and are used only by `UpdateRecord`. That method tombstones matching
records and appends a new version within the same transaction.

A caller can provide a `TransactionContext` to group work. Otherwise, table operations create,
complete, and dispose a transaction themselves. Calling `Complete` or `CompleteAsync` commits;
disposing an open transaction rolls it back. Database contexts are `IAsyncDisposable` and must be
released with `await using`.

For a compact example, start with `TrackDb.UnitTest\IntegrationTest.cs`. For schemas, primary
keys, and policy setup, use `TrackDb.UnitTest\DbTests\TestDatabase.cs`.