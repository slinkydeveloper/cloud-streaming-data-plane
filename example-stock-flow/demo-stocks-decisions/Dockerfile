FROM scratch

# To build it: cargo build --target x86_64-unknown-linux-musl --release
COPY target/x86_64-unknown-linux-musl/release/function /app

CMD ["/app"]
