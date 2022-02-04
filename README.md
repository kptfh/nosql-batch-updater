[![CircleCI](https://circleci.com/gh/Playtika/nosql-batch-updater/tree/develop.svg?style=shield&circle-token=2fee7a6c26cbd37d4530dc0ef8d4f70027d070ae)](https://circleci.com/gh/Playtika/nosql-batch-updater/tree/develop)
[![codecov](https://codecov.io/gh/Playtika/nosql-batch-updater/branch/master/graph/badge.svg)](https://codecov.io/gh/Playtika/nosql-batch-updater)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.playtika.nosql/batch-updater-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.playtika.nosql/batch-updater-parent)
# nosql-batch-updater
Allows to run batch updates on NoSql DBs with eventually consistent guarantee. 
Some NoSql DBs (like Cassandra) already have built-in batch update mechanism but most of them have no such option. 
This library allows to use batch updates on any NoSql (Key-Value) DB.

Known limitations:
- updates should be idempotent
- size of batch depends on max size of record WriteAheadLog can store

As for now it supports Aerospike batch updates only
