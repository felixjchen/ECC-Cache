# Distributed-Cache
Created a in memory key value store, using two strategies:
1. Replication using Raft
2. Error Correcting codes using Reed Solomon

## Assumptions
- clients are healthy for the duration of a transacation

## Todo overall
- common config files
- common cli
- error handling 
- integration tests

## Todo replication
- error handling 
- integration tests
- Storage seperate
- Storage brings only value into mem
- client

## Todo ecc
- optomistic concurrency writes
- config files
- restore