# Roadmap

## Partitioning

A future partitioning feature could designate table columns as partition keys. Every block in a
partition would then share values for those columns, allowing queries to prune blocks earlier and
allowing efficient deletion of complete partitions.

## Further Block Merge

Additional scheduled merge strategies could combine blocks that are undersized because of initial
persistence or later hard deletion. This is less urgent for workloads that logically update records
frequently, because their older versions tend to be tombstoned and their blocks can be released.