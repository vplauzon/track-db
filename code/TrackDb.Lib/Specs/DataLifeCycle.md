# Data Life Cycle

This page details the life cycle of committed data. Data is first retained in memory and is then
persisted to blocks. This reduces disk writes, which is particularly important for SSD-backed
storage, while balancing memory pressure against persistence work.

## Activities

The lifecycle manager performs these categories of work:

* In-memory transaction-log maintenance.
* Record persistence to blocks.
* Hard deletion of tombstoned records.
* Metadata-block merging and block maintenance.

## Components

* **Persist non-metadata records:** `RecordPersistanceAgent` persists older in-memory records.
* **Persist metadata records:** `RecordPersistanceAgent` persists metadata-table records.
* **Hard delete records:** `HardDeleteAgent` removes records based on tombstone age or volume.
* **Merge metadata blocks:** `MetaBlockMergingLogic` combines undersized metadata blocks.
* **Merge transaction logs:** `TransactionLogMergingAgent` limits query and allocation overhead.
* **Release cached blocks:** `BlockCacheManager` releases blocks that are no longer in use.

For the hard-delete and block-merge behavior, see [Block maintenance](BlockMaintenance.md).