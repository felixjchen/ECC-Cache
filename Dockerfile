FROM rust:1.53 as builder
WORKDIR /usr/src/myapp
RUN rustup component add rustfmt 
COPY . .
RUN cargo install --path .

FROM debian:buster-slim
EXPOSE 50051
COPY --from=builder /usr/local/cargo/bin/distributed-cache-server /usr/local/bin/distributed-cache-server
CMD ["distributed-cache-server"]

