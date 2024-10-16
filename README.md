# Scylla tools

This repository contains `nodetool` and other Apache Cassandra compatible tools for Scylla.

Please refer to the [Scylla git repository](https://github.com/scylladb/scylla) for more information how to build and run the tools.

# Deprecation Notice

The tools contained in this repository are considered deprecated and are slated for removal.
Starting from ScyllaDB 6.3, these tools will not be included in the ScyllaDB base packages, instead they will be available as standalone packages for some time, then removed entirely at an unspecified point in time in the future.

Note: cassandra-stress continues to be supported and available.

Below is a list of tools and their native successor, as well as the ScyllaDB version starting from which the native successor is available.

| Tool                   | Native Successor                          | Available From                |
| ---------------------- | ----------------------------------------- | ----------------------------- |
| nodetool               | scylla nodetool                           | 6.0                           |
| scylla-sstableloader   | scylla nodetool refresh --load-and-stream | 4.6                           |
| sstabledump            | scylla sstable dump-data                  | 5.4                           |
| sstableexpirebblockers | no successor                              | N/A                           |
| sstablelevelreset      | no successor                              | N/A                           |
| sstableloader          | scylla nodetool refresh --load-and-stream | 4.6                           |
| sstablemetadata        | scylla sstable dump-statistics            | 5.4                           |
| sstablerepairedset     | no successor                              | N/A                           |
| sstablescrub           | scylla sstable scrub                      | 5.4                           |
| sstablesplit           | no successor                              | N/A                           |
| sstableupgrade         | no successor                              | N/A                           |
| sstableutil            | no successor                              | N/A                           |
| sstableverify          | scylla sstable validate                   | 5.4                           |
