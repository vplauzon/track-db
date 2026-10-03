# Architectural Decisions

These decisions define the system's intended invariants. Preserve them unless a change explicitly
revises the architecture and its associated tests and documentation.

## Records Are Never Updated In Place

`UpdateRecord` tombstones matching primary-key records and appends a new version in the same
transaction. Do not add a direct block-mutation path for updates.

## In-Memory State Leads Local Blocks

Committed transaction data is available through in-memory logs before lifecycle processing persists
it to local blocks. Queries must include applicable in-memory and persisted data in their snapshot.

## Local Blocks Are Ephemeral; Azure Logs Are Durable

The local block file is a temporary working store. When configured, Azure Storage transaction logs
provide durable history for rehydration; do not assume local blocks survive a restart.

## Blocks Are Immutable and Column-Oriented

Column statistics prune blocks and projection avoids unnecessary decoding. Preserve column order,
statistics, and block replacement semantics when changing encoding or query execution.

## Data Lifecycle Work Is Asynchronous and Policy-Driven

The lifecycle manager merges logs, persists records, hard deletes tombstones, and maintains blocks.
`DatabasePolicy` controls timing and thresholds; persisted state may require forced lifecycle work.

## Metadata Connects Logical Tables to Physical Blocks

Metadata blocks form a hierarchy over data blocks. Each metadata block records the range of values
for every column of each child block, enabling queries to prune nonmatching blocks. Tombstones and
replacement blocks must preserve this metadata; changing a child requires ancestor updates to root.

## Typed Schemas Define the Public Data Contract

Typed schemas derive columns from record constructors. Keep schema mapping, predicates, and
serialization aligned when adding types or changing the typed API.