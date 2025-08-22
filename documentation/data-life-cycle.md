# Data Life Cycle

This page details the life cycle of data.

Data is first cached in memory before being persisted to disk.  This is done to minimize the number of disk-writes which can deteriorate solid state drives.  It therefore becomes a balancing act between memory pressure and writes.

## Activities

The activity categories are the following:

*   In memory cache maintenance
*   Persist data
*   Persist deletion
*   Block merge

The details is as follow:

Activity|Priority|Description
-|-|-
Transaction log merge|P0|In-memory transaction log merges.  This is done to have a more efficient data structure.
Persist old data|P0|This is triggered when too much data is in the cache.  There are two triggers for that.  The first one is when the total records in user tables is over a threshold.  The second is when the record count on any metadata table is over a threshold.  The oldest data is persisted to blocks first.  This is done to relieve memory pressure.  I.e. we persist the block with the oldest record ID and reassess.
Delete whole blocks|P0|This is triggered when too many record IDs are marked for deletion.  This is quite efficient as we do not need to re-write blocks, simply detect the whole block is deleted and remove it from the table.  This is done to relieve memory pressure but also avoid false query hits.
Delete records|P1|This is triggered when too many record IDs are still marked for deletion.  Blocks are rewritten without deleted record IDs.  We start with oldest records.  Done to relieve memory pressure.
Delete old records|P2|This is triggered every 2 minutes.  Essentially we give 2 minutes for the records of an entire block to be deleted before re-writing them.  Done to avoid false query hits.
Block merge|P3|Triggered every 5 minutes.  Small blocks (less than %50 used) are first identified, then we identify "partner blocks" to merge them with.  We then merge by re-writing.  Done to optimize query reads and keep metadata in check.

## Order of activities

Looking at the activities, we want them to be run in the presented order.  The first four relieve memory pressure and must be run in due time while the last 2 can wait and be run in second priority.

It is simpler, in terms of implementation to run those sequentially as it avoids to deal with parallelism.

The implementation starst from the top and goes down.  Each time a change is done at one step, we restart the sequence.  This ensures we always work on the freshest data and that we attack the activity with the most priority.
