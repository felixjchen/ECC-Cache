# Distributed-Cache
Created two in memory key value stores, using two strategies:
1. Raft replication
2. Error Correcting Codes (Reed Solomon)

## Motivation

## Assumptions
- clients are healthy for the duration of a transacation
- for ecc cache, transactions are atomic
- for ecc cache, let value = utf8 encoding of value, then |value| <= k * block_size

## Todo overall
- error handling 
- integration tests
- benchmarking
- different configjsons
- cleanup logging

## Todo raft
- Storage hard state
- check snapshotting logic

## Todo ecc
- heartbeats
- 2PC
- restore (no in flight transactions? yep no in flight, only healthy node with empty WAL)
- restore needs to crawl all neighboors
- better client naming
- better timeout

## Week 1
- sketched outline for ECC cache
- project setup
- learned about gRPCs (tokio), Raft in Rust (tikv rust, async rust...)
- implemented the networking trait for async-rust, to create a raft k/v store that uses gRPCs

## Week 2
- created ECC client / server
  - Servers contain key value maps
    - Each server contains a block, where k*block_size = |message|, **we only need k blocks to reconstruct the message**
    - Reed Solomon requires Galois field 2^8... our message needs to be base 256. UTF8 does this for us, common characters have 1 char - 1 number in base256, rare characters are less efficient
    - Can recover if told so
  - Client ECC code
    - Reads first k responses, constructs message
    - Write to as many servers as possible (?) (missing optomistic concurrency)
- cleaned up Raft implementation
  - Created client.rs, instead of using BloomRPC to test rpcs
  - No more stale reads, all reads are fresh from leader
  - Writing finds the leader
  - Among other code improvements
- unified CLI entrypoint
  - cargo run
    - ecc
      - server
        - startAll
        - startOne
          - recover
      - client
        - set 
          - k 
          - v
        - get k
    - raft
      - server
        - startAll
      - client
        - set
          - k 
          - v
        - get
          - k

## Week 3 (TODO)
- Choose a testing framework, strategy that can monitor resources and time
- 2PC + Restore 
- Raft membership changes
- Report 