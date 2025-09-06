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

Activity|Priority|Description
-|-|-
Transaction log merge|P0|In-memory transaction log merges.  This is done to have a more efficient data structure.  There is a balance between efficiency and churn so we wait for time or number of logs to merge so we are not continuously merging (i.e. newing arrays and copying).
Persist old data|P0|This is triggered when too much data is in the cache.  There are two triggers for that.  The first one is when the total records in user tables is over a threshold.  The second is when the record count on any metadata table is over a threshold.  The oldest data is persisted to blocks first.  This is done to relieve memory pressure.  I.e. we persist the block with the oldest record ID and reassess.
Hard deletion|P0|Triggered when too many records are tombstoned or after 2 minutes of being tombstoned.  The former release relieve memory pressure while, the latter improves performance by avoiding false query hits.
Block merge|P3|Triggered every 5 minutes.  Small blocks (less than %50 used) are first identified, then we identify "partner blocks" to merge them with.  We then merge by re-writing.  Done to optimize query reads and keep metadata in check.

## Order of activities

Looking at the activities, we want them to be run in the presented order.  The first four relieve memory pressure and must be run in due time while the last 2 can wait and be run in second priority.

It is simpler, in terms of implementation to run those sequentially as it avoids to deal with parallelism.

The implementation starst from the top and goes down.  Each time a change is done at one step, we restart the sequence.  This ensures we always work on the freshest data and that we attack the activity with the most priority.
