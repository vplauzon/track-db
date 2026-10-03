# Development Guide

## Where to Change Things

| Concern | Start with |
| - | - |
| Database API and lifecycle | `Database.cs` and `TransactionContext.cs` |
| Typed table API | `TypedTableSchema.cs`, `TypedTable.cs`, and `TypedTableQuery.cs` |
| Untyped table mutation and query execution | `Table.cs` and `TableQuery.cs` |
| Predicates and query pruning | `Predicate\` and `InMemory\Block\` |
| In-memory transaction state | `InMemory\` |
| Persisted block lifecycle | `DataLifeCycle\` and `DataLifeCycle\Persistance\` |
| Block encoding | `Encoding\` |
| Durable log format and recovery | `Logging\` |
| Tuning thresholds and storage options | `Policies\` |

Trace both the direct operation and lifecycle consequences. Mutation changes can affect tombstones,
transaction-log merging, persistence, hard deletion, log recovery, and metadata-block maintenance.

## Tests and Local Commands

From the repository root, the continuous-build workflow uses:

```powershell
dotnet restore code
dotnet build code\ --configuration Release --no-restore
dotnet test code\TrackDb.UnitTest --configuration Release --no-build --verbosity normal
```

Use `DbTests` for table, query, block, lifecycle, and trigger behavior. Use `TrackDb.LogTest` for
log persistence and rehydration, and `TrackDb.PerfTest` for performance-sensitive paths. Tests
commonly force lifecycle activities to make persistence deterministic.