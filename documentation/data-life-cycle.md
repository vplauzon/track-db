# Data Life Cycle

This page details the life cycle of data.

Data is first cached in memory before being persisted to disk.  This is done to minimize the number of disk-writes which can deteriorate solid state drives.  It therefore becomes a balancing act between memory pressure and writes.

## Activities

The activity categories are the following:

*   In memory cache maintenance
*   Persist data
*   Hard deletion
*   Block merge

The details is as follow:

Activity|Component|Description
-|-|-
Release blocks|ReleaseBlockAgent|Releases blocks that are no longer used.
Hard deletion (record count)|RecordCountHardDeleteAgent|Triggered when too many records are tombstoned and is done to relieve memory pressure.  Merge with neighbouring block is done at the same time.
Persist records (non-metadata)|NonMetaRecordPersistanceAgent|Triggered by too much data (non metadata) in the cache and is done to relieve memory.  The oldest data is persisted to blocks first.
Hard deletion (time based)|TimeHardDeleteAgent|Triggered when a record has been tombstoned for too long.
Block merge (1st generation)|TODO|Triggered just before a group of block metadata gets persisted to maximize block size and minimize generations.
Persist records (metadata)|NonMetaRecordPersistanceAgent|Triggered by too much metadata (non metadata) in the cache and is done to relieve memory.  The oldest data is persisted to blocks first.
Transaction log merge|TransactionLogMergingAgent|In-memory transaction log merges.  This is done to have a more efficient data structure.  There is a balance between efficiency and churn so we wait for time or number of logs to merge so we are not continuously merging (i.e. newing arrays and copying).

## Block merge & Hard delete

Blocks can get fragmented:

* Blocks can be created with sub maximal size
* Blocks can get smaller and smaller with hard deletes

Hard deletes delete records "in place", that is a new block is created without the tombstoned records and replaces the old block.  This requires updating the block at the level above, recursively.

The "in place" is done to preserve record-id ordering in blocks with the heuristic that blocks created at the same time tend to have similar keys, hence improving pruning at query time.

To avoid fragmentation, block merge is run in different operations:

* Hard delete (both record & time based)
* Before metadata persistance
* At schedule time, just to optimize blocks

## Block Merge Algorithm

This is always done in a metadata table, either of generation 2 or above.

This is done with all the blocks of the same metablock in the level above or all the blocks in-memory.

Blocks are first sorted in order of minimum-record-IDs.  Starting from the smallest values of record ids, we are going to move right.  At each step, we check if the left block can merge with the right block.  If it can, we merge in-memory.  If it can't, if the left block was in-memory, it is persisted.  Once we are done we have a new sequence of block IDs.