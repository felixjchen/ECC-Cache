# Distributed-Cache
Created two in memory key value stores, using two strategies:
1. Raft replication
2. Error Correcting Codes (Reed Solomon)

## Motivation

## Assumptions
- clients are healthy for the duration of a transacation

## Todo overall
- error handling 
- integration tests
- benchmarking
- cleanup logging

## Todo raft
- client
- Storage hard state
- bad leader writes
- 

## Todo ecc
- optomistic concurrency writes
- restore (no in flight transactions?)
- better timeout
