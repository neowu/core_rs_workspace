FROM        rust:trixie AS builder
WORKDIR     /usr/src
RUN         curl https://packages.confluent.io/deb/7.9/archive.key | gpg --dearmor > /usr/share/keyrings/confluent.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/confluent.gpg] https://packages.confluent.io/clients/deb bookworm main" | tee /etc/apt/sources.list.d/confluent.list
RUN         apt-get update && apt-get install -y librdkafka-dev libclang-dev && rm -rf /var/lib/apt/lists/*
COPY        . .
RUN         --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/target \
    cargo build --release --bin log_processor && cp target/release/log_processor /usr/local/bin/log_processor

FROM        debian:trixie-slim
COPY        --from=builder /etc/apt/sources.list.d/confluent.list /etc/apt/sources.list.d/confluent.list
COPY        --from=builder /usr/share/keyrings/confluent.gpg /usr/share/keyrings/confluent.gpg
RUN         apt-get update && apt-get install -y librdkafka-dev ca-certificates && rm -rf /var/lib/apt/lists/*

RUN         mkdir -p /opt/app
COPY        --from=builder /usr/local/bin/log_processor /opt/app/
COPY        --from=builder /usr/src/app/log_processor/assets /opt/app/assets

RUN         groupadd --gid 1000 app && useradd --no-create-home --gid app --uid 1000 app
USER        1000

CMD         ["/opt/app/log_processor"]
