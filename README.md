# nosql-batch-updater
Allows to run batch updates on NoSql DBs with eventually consistent guarantee. 
Some NoSql DBs (like Cassandra) already have built-in batch update mechanism but most of them have no such option. 
This library allows to use batch updates on any NoSql (Key-Value) DB.

Known limitations:
- updates should be idempotent
- size of batch depends on max size of record WriteAheadLog can store

As for now it supports Aerospike batch updates only
