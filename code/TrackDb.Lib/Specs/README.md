# TrackDb Specs

## Purpose

TrackDb is a .NET in-process database and a learning project for database implementation
concepts. It targets small, frequently changing datasets that are too large to retain entirely
in memory. A database owns multiple strongly typed or untyped tables and shares transactions
across them.

Use the implementation and its tests as the source of truth for current behavior. This directory
contains the project documentation used by contributors and agents.

## Start Here

* [Coding standard](CodingStandard.md) before modifying C# or Markdown files.
* [Architectural decisions](Architecture.md) for invariants that changes must preserve.
* [Core model](CoreModel.md) for database, table, schema, and transaction entry points.
* [Storage model](StorageModel.md) for records, blocks, local storage, and durable logs.
* [Runtime data flow](RuntimeDataFlow.md) for committed-data processing and query execution.
* [Data life cycle](DataLifeCycle.md) and [block maintenance](BlockMaintenance.md) for background
  persistence, deletion, and merging.
* [Development guide](DevelopmentGuide.md) for change locations, tests, and local commands.
* [Roadmap](Roadmap.md) for planned capabilities.

## Solution Map

| Project | Responsibility |
| - | - |
| `TrackDb.Lib` | Core database, query, storage, logging, and lifecycle implementation. |
| `TrackDb.UnitTest` | Unit and integration coverage for database behavior. |
| `TrackDb.LogTest` | Log persistence and database rehydration coverage. |
| `TrackDb.PerfTest` | Performance and volume scenarios. |

The solution targets `net10.0`, enables nullable reference types, and treats all warnings as
errors. The core project depends on Azure Data Lake storage for optional logging and FusionCache
for block caching.