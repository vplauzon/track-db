# Index structure

Ipdb's model is a file-on-disk per database with an optional Azure Blob audit log.  A database contains multiple tables.  Each table can store multiple records.

Records can be queried from a table and they can be appended or deleted.

A record is mapped to a .NET object and hence contain a number of property values.  A table has an optional primary key which is one or many properties.  When a table has a primary key, when a record is appended, if a record with the same primary key exist, it is deleted before insertion automatically.

Records are stored in blocks of 4 KBs on disk.  Each block contains records from only one table.  The block statistics are stored in memory only and contain the min and max of each property.  This allows queries to skip blocks when the query search for values outside a block's statistics.

Tables block lists are kept in memory but loaded when needed.

Blocks are read only but are recycled when they are replaced and no active transaction use them.

New records are persisted to new blocks.  When records are deleted, a new block without those records is created (unless all records in the block were deleted).

Database has an in-memory cache of records being appended and deleted.  The cache is flushed to disk when it exceeds certain threshold.  The cache minimizes writes to disk:

*   A record might be deleted before it's ever written to disk
*   Multiple records will be written together in a single write

##  Data Model

Record properties can be of type:

* `int`

##  Record ID

As an implementation detail, each record is identified by a row-id of type `long`, unique accross all tables in the database.  It is simply incremented from `1`.

Having a record ID allows us to keep a list of record id to delete.

## Block Format

Each record has the following fields:

Name|Type|Description
-|-|-
Revision ID|`long`|Document's revision ID
Length|`short`|Serialized document Length
Document|`char[]`|Serialized (in JSON) document

##  Transactions

Each query and data manipulation (append & delete)

##  Partitioning

##  Merge

