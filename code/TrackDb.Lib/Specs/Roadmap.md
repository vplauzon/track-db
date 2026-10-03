# Roadmap

## Better scalability

Objective would be to support 100 million records in a single table.  Currently that is possible
but very slow.

## Improve load time

When a database is rehydrated from Azure blob, as soon as it has a few thousand records, it can
be pretty long to do so.

One of the innefficiency is that we replay the entire log (unless it was compacted).  Since we
download the logs on local disks, could we do something better?  For instance, could we do two
passes, one to read the deleted record-IDs (and persist them locally) and then another pass to
load records but discarding those that get deleted?  This would be less loading, deletion and
merging.

## Partitioning

A future partitioning feature could designate table columns as partition keys. Every block in a
partition would then share values for those columns, allowing queries to prune blocks earlier and
allowing efficient deletion of complete partitions.

## Further Block Merge

Additional scheduled merge strategies could combine blocks that are undersized because of initial
persistence or later hard deletion. This is less urgent for workloads that logically update records
frequently, because their older versions tend to be tombstoned and their blocks can be released.