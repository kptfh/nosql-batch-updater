[![codecov](https://codecov.io/gh/Playtika/nosql-batch-updater/branch/master/graph/badge.svg)](https://codecov.io/gh/Playtika/nosql-batch-updater)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.playtika.nosql/batch-updater-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.playtika.nosql/batch-updater-parent)
# nosql-batch-updater
Allows to run batch updates on NoSql DBs with eventually consistent guarantee. 
Some NoSql DBs (like Cassandra) already have built-in batch update mechanism but most of them have no such option. 
This library allows to use batch updates on any NoSql (Key-Value) DB.

Known limitations:
- updates should be idempotent
- size of batch depends on max size of record NoSql DB can store

As for now it supports Aerospike batch updates only
