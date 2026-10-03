# Storage Model

TrackDb is intended for small, frequently changing databases, typically a few MB in size. Workflow
state is a representative workload: it can contain many records, changes frequently, and benefits
from local query performance without requiring the data to remain entirely in memory.

Each database owns multiple tables. Its local file is a temporary working store, such as one inside
a container. When Azure Storage logging is configured, its transaction logs provide the long-term
durable history used to rehydrate the database.

## Data Model

A table has a strongly typed column schema. A typed table maps a .NET representation type to that
schema by using the parameters of the type's constructor as columns. Supported scalar column types
include `byte`, `short`, `int`, `long`, `bool`, enums, `DateTime`, `string`, and `Uri`.

Each non-metadata record receives a monotonically increasing `long` record ID from its table. The
record ID allows TrackDb to represent deletion as a tombstone without modifying persisted data.

## Data Blocks and File Blocks

A **data block** is the immutable, column-oriented container for records from one table. A
**file block** is a fixed-size allocation in the local database file; its default size is 4 KiB and
is configured through `StoragePolicy`.

Metadata records the minimum and maximum value of every column in each data block. Queries use
those ranges to skip data blocks that cannot match.

Metadata is itself stored as a hierarchy of blocks. Each persisted table, user or metadata, has a
metadata table that describes its blocks. A metadata table has a table generation one above the
table it describes (user tables are generation one), and the next level is created on demand when
that metadata table's own records are persisted. There is no fixed limit on the number of levels.

Metadata records are held in memory only until persisted, which the lifecycle manager does once
in-memory metadata records exceed `InMemoryPolicy.MaxMetaDataRecords`. The in-memory footprint of
metadata is therefore bounded by that policy, not by data volume; the topmost level holds the
records that are not persisted yet. Persisted metadata and data blocks are loaded on demand through
`BlockCacheManager`, which keeps metadata blocks at a higher cache priority. Do not read this
section as "all metadata is resident in memory".

Data blocks remain immutable for active transactions; replacing one waits until no active
transaction still uses it. New records create new data blocks, while deletion creates a replacement
without deleted records unless the complete data block is removed.

Each query and data manipulation is part of a transaction. A transaction snapshots block structure
and in-memory state, then contributes appended records and tombstones when it commits.