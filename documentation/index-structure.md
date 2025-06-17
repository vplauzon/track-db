# Index structure

Ipdb has a model where a database contains multiple tables.  Each table can store multiple documents.  It uses hash indexes to support equality search in indexes, e.g. `where id==5`.

Each document has a primary index and optional secondary indexes.  The primary index uniquely identifies the document.

A key (primary of secondary) can be of type:

* `int`
* `long`
* `Enum`
* `string`
* A tuple of arbitrary length with a combinaison of the previous types

As an implementation detail, each document revision is identified by a revision-id of type `long`, unique accross all tables in the database.  It is simply incremented from `1`.  This choice has the property of streamlining the insertion/update of multiple documents.

All documents and indexes for all table of a database are stored in one file (`ipdb.data`).  The file is divided into 4 KBs block.  A block map is managed by a `Manager` which reserves them and release them (when all data in it is empty) to a `StorageManager`.

The list of blocks is kept in memory by the managers.

Block type |Manager |Key| Block belongs to|Data
-|-|-|-|-
Document|DocumentManager|Revision ID|Database|IsDeleted, JSON document
Index|IndexManager|Primary/Secondary Key|Table|IsDeleted, Key hash, Active revision ID list with that key hash

Within the block map of `Manager` is kept the range of active (non deleted) keys it has.  It is therefore `O(log(# blocks))` to find a block, but `O(# records in a block)` to find the key within the block as blocks are not ordered.

When a block isn't big enough to contain a new key, it is split (and cleaned up from its deleted items) into two pieces.

When a document is deleted, its entries get marked deleted in all block types.

When a document is appended, it is first virtually deleted then inserted.  We say virtually because optimizations are possible such as changing a key in place.

When a query with more than one key in filter, e.g. `where id==5 and colour=="blue"`, the key hashes are used to find a list of revision ids.  Logic arithmetic is applied to the revision id lists (e.g. union, intersection, etc.) then documents are found from document blocks, documents are deserialized and full keys are used to double check the predicate (avoid hash clash).

## Block Format

### Document

Each record has the following fields:

Name|Type|Description
-|-|-
Revision ID|`long`|Document's revision ID
Length|`short`|Serialized document Length
Document|`char[]`|Serialized (in JSON) document

### Index

Each record has the following fields:

Name|Type|Description
-|-|-
Key hash|`short`|Hash of the index key
Revision ID|`long`|Document's revision ID

Transient errors during listing
Bulk table retention
Test coverage
