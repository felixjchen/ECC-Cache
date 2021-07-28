FROM rust:1.53 as builder
WORKDIR /usr/src/myapp
RUN rustup component add rustfmt 
COPY . .
RUN cargo install cargo-quickinstall
RUN cargo quickinstall --path .

FROM debian:buster-slim
COPY ecc_config.json .
COPY raft_config.json .
COPY --from=builder /usr/local/cargo/bin/distributed_cache /usr/local/bin/distributed_cache
ENTRYPOINT ["distributed_cache"]

