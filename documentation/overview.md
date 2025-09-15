# Overview

TrackDb targets small, in-process databases that change frequently. Typical size is a few MBs — small enough to fit on disk, but large enough to create memory pressure if kept entirely in RAM.

A good example for a use case is workflow states:  this can be arbitrarily large but not huge and also have its records being updated all the time.

Each database is persisted with a file-on-disk.  That file is meant to be temporary (e.g. part of a Docker container) while an optional Azure Blob audit log is the long term persistancy solution.  A database contains multiple tables.  Each table can store multiple records.

Records can be queried from a table and they can be appended or deleted.

A table has a strong schema represented by a column (with column type) list.  The table is mapped to a .NET type called a *representation type* which dictates its schema.  Only record types are supported where the constructor parameters represent the columns.

Tables do not have primary key and hence do not support updates:  only append and delete.

Records are stored in blocks of 4 KBs on disk.  Each block contains records from only one table.  The block statistics are stored in memory only and contain the min and max of each record columns.  This allows queries to skip blocks when the query search for values outside a block's statistics.

Tables block lists are kept in memory but the actual blocks are loaded on demand.

Blocks are read only but are recycled when they are replaced and no active transaction use them.

New records are persisted to new blocks.  When records are deleted, a new block without those records is created (unless all records in the block were deleted).

Database has an in-memory cache of records being appended and deleted.  The cache is flushed to disk when it exceeds certain threshold.  The cache minimizes writes to disk:

*   A record might be deleted before it's ever written to disk
*   Multiple records will be written (or deleted) together in a single write

##  Data Model

Record columns can be of type:

* `byte`
* `short`
* `int`
* `long`
* `enum`
* `datetime`
* `string`

##  Record ID

As an implementation detail, each record is identified by a row-id of type `long`, unique accross all tables in the database.  It is simply incremented from `1`.

Having a record ID allows us to keep a list of record id to delete.

## Block Format

Each block contains a list of records from the same table.  Records are stored column by column.

Having a columnar format allows us to more easily implement compression.  It also allows us to decode one column at the time for query filtering:  if no record in a block is selected, only the filter columns are decoded.

##  Transactions

Each query and data manipulation (append & delete) is part of a transaction.  Transaction carries a snapshot of the block structure and state of the cache.  It appends more cache (e.g. new records or record deletion) which are merged to the database cache upon commit.

## Roadmap

###  P1:  Partitioning

The feature of partitioning would allow a user to define a subset of columns for each table defining a partition.  Each block belonging to that table would then have a single value for those columns.

This would speed up queries when the filter match the partition columns.  It would also allow more efficient deletion if partitions are deleted wholesales.

###  P2:  Merge

We could routinely merge blocks where either records were previously deleted or the block was written without being full.

This is likely not necessary for workloads where records get updated (i.e. deleted then recreated within the same transaction) all the time.  Old records would likely be deleted quickly, freeing the entire blocks.