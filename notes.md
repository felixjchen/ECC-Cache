## Standard
https://en.wikipedia.org/wiki/Redis
https://redis.io/topics/cluster-tutorial
https://github.com/tikv/tikv
https://tikv.org/deep-dive/scalability/multi-raft/

## Shard Rebalancing
https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/redis-cluster-resharding-online.html
https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/Shards.html

## Optimistic Concurrency Control
https://stackoverflow.com/questions/129329/optimistic-vs-pessimistic-locking
https://docs.databricks.com/delta/concurrency-control.html
https://en.wikipedia.org/wiki/Optimistic_concurrency_control

## Reed Solomon
https://github.com/darrenldl/reed-solomon-erasure

## Rust Tokio Async
https://tokio.rs/tokio/tutorial/async

## Await first k
https://stackoverflow.com/questions/68448854/how-to-await-for-the-first-k-futures

## Str to base 256
https://doc.rust-lang.org/stable/std/string/struct.String.html#method.into_bytes
https://doc.rust-lang.org/std/str/fn.from_utf8.html

## Redis uses a map internally
db.c => dbAdd => dictAdd...

##
docker run -e DOCKER_HOSTNAME=host.docker.internal felixchen1998/distributed-cache-server:latest ecc client set key2345 3333

docker run -e DOCKER_HOSTNAME=host.docker.internal felixchen1998/distributed-cache-server:latest raft client set key2345 testfdasfdas

## Docker routing
https://dev.to/natterstefan/docker-tip-how-to-get-host-s-ip-address-inside-a-docker-container-5anh