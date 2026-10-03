# Block Maintenance

## Block Merge and Hard Delete

Blocks can become fragmented when they are created below their maximum size or when hard deletes
remove records. A hard delete creates a replacement block that excludes tombstoned records; it then
updates the containing metadata hierarchy recursively. The replacement preserves record-ID ordering
so blocks created around the same time remain useful for query pruning.

Block merge runs during hard deletion, before metadata persistence, and as scheduled maintenance.
It reduces fragmentation by combining compatible blocks into larger blocks.

## Block Merge Algorithm

Block merge operates on a metadata table at generation two or higher. It considers all blocks of
the same parent metadata block, or all in-memory blocks when no persisted parent exists.

The algorithm orders blocks by minimum record ID and walks from left to right. Compatible
neighbouring blocks are merged in memory. When blocks cannot merge, an in-memory left block is
persisted before processing continues. The result is a replacement sequence of block IDs.