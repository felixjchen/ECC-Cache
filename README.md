# Distributed-Cache

🗞️ [Report](https://github.com/felixjchen/Distributed-Cache/blob/main/report/report.pdf)

📊 [Benchmarks](https://github.com/felixjchen/Distributed-Cache/tree/main/report/benchmarks)

🎓 [Course / Studies](https://github.com/felixjchen/D94/blob/main/README.md)

## Motivation
Reduce redundancy in distributed caching by avoiding data replication and applying error correction codes. 


## Result

Created two distributed key value stores, using two strategies:
1. Raft replication
2. Error Correcting Codes (Reed Solomon)

To store 10,000 unique key-value pairs:
### ECC cache uses 3.5 MiB
![](https://user-images.githubusercontent.com/31393977/129127326-b744db92-29ca-4881-8aee-98c308f8b958.png)
### Raft based cache uses 6.0 MiB
![](https://user-images.githubusercontent.com/31393977/129127327-3d3aedab-76d6-4240-8225-d92d7a13cc78.png)


## Running From Source

```
git clone https://github.com/felixjchen/Distributed-ECC-Cache
cd Distributed-ECC-Cache
cargo build

cargo run ecc server startAll
cargo run ecc client set fruit cherry
cargo run ecc client get fruit
```
- The command tree can be found in week 2 

# Implementation

## Assumptions
- clients are healthy for the duration of a transacation
- for ecc cache, transactions are atomic
- for ecc cache, let value = utf8 encoding of value, then |value| <= k * block_size

## Todo overall
- error handling 
- integration tests
- cleanup logging

## Todo raft
- Storage hard state
- Handling getting from dead servers

## Todo ecc
- better client naming and code reuse
- 2PC may be buggy / testing
- better timeout https://tokio-rs.github.io/tokio/doc/tokio/time/fn.timeout.html
- Handling getting from dead servers

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

## Week 3 
- Heartbeats, 2PC , Better Restore
- Raft start one / client improvements
- Dockerize everything, docker-compose and compiling the project
- Figured out cAdvisor for resource monitoring
- Tried netstat for resource monitoring
- Wrote benchmarking tool

## Week 4
- Report
- Benchmarking workload b
- 2PC + Restore 
- Raft membership changes
- Report 
- Benchmarking maybe on a fresh VM on digital ocean ?
- trying to get benchmark with errors
