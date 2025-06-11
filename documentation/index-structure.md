# Index structure

Ipdb uses hash indexes and hence supports equality search in indexes, e.g. `where id==5`.

Each document has a primary index and optional secondary indexes.  The primary index uniquely identifies the document.

A key (primary of secondary) can be of type:

* `int`
* `long`
* `Enum`
* `string`
* A tuple of arbitrary length with a combinaison of the previous types

As an implementation detail, each document revision is identified by a revision-id of type `Guid`.  It is unique accross all tables in the database.  Being a random Guid, it has the property of minimizing contention (as opposed to an incrementing long) when multiple documents are inserted.

All data for a database is stored in one file (`ipdb.data`).  The file is divided into 4 KBs block.  A block map is managed by a `Manager` which reserves them and release them (when all data in it is empty) to a `StorageManager`.

The list of blocks is kept in memory by the managers.

Block type |Manager |Key| Block belongs to|Data
-|-|-|-|-
Document|DocumentManager|Revision ID|Database|IsDeleted, JSON document
Index|IndexManager|Primary/Secondary Key|Table|IsDeleted, Key hash, Active revision ID list with that key hash

Within the block map of `Manager` is kept the range of active (non deleted) keys it has.  It is therefore `log(n)` to find a block, but `n` to find the key within the block as blocks are not ordered.

When a block isn't big enough to contain a new key, the block is split (and cleaned of its deleted items) into two pieces.

When a document is deleted, its entries get marked deleted in all block types.

When a document is appended, it is first virtually deleted then inserted.  We say virtually because optimizations are possible such as changing a key inplace.

When a query is executed that requires more than one key, e.g. `where id==5 and colour=="blue"`, the key hashes are used to find a list of revision ids.  Logic arithmetic is applied to the lists then documents are found from document blocks, documents are deserialized and full keys are used to double check the predicate.

## Block Format

### Document

Each document has the following layout:

* Length of document:  `short`
* JSON document:  `string`

### Index

Each index has the following layout:

* Key hash:  short
* Length of key:  `short`
* Key (in JSON):  `string`
