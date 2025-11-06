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
Block merge (1st generation)|TODO|Triggered just before a group of block metadata gets persisted to maximize block size and minimize generations.  Blocks are merged together until under threshold.
Persist records (metadata)|NonMetaRecordPersistanceAgent|Triggered by too much metadata (non metadata) in the cache and is done to relieve memory.  The oldest data is persisted to blocks first.
Transaction log merge||In-memory transaction log merges.  This is done to have a more efficient data structure.  There is a balance between efficiency and churn so we wait for time or number of logs to merge so we are not continuously merging (i.e. newing arrays and copying).

## Order of activities

Looking at the activities, we want them to be run in the presented order.  The first four relieve memory pressure and must be run in due time while the last 2 can wait and be run in second priority.

It is simpler, in terms of implementation to run those sequentially as it avoids to deal with parallelism.

The implementation starst from the top and goes down.  Each time a change is done at one step, we restart the sequence.  This ensures we always work on the freshest data and that we attack the activity with the most priority.
