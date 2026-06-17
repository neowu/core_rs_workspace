FROM        rust:trixie AS chef
RUN         cargo install --locked cargo-chef
WORKDIR     /usr/src

FROM        chef AS planner
COPY        . .
RUN         cargo chef prepare --recipe-path recipe.json

FROM        chef AS builder
RUN         curl https://packages.confluent.io/deb/7.9/archive.key | gpg --dearmor > /usr/share/keyrings/confluent.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/confluent.gpg] https://packages.confluent.io/clients/deb bookworm main" | tee /etc/apt/sources.list.d/confluent.list
RUN         apt-get update && apt-get install -y librdkafka-dev libclang-dev && rm -rf /var/lib/apt/lists/*
COPY        --from=planner /usr/src/recipe.json recipe.json
RUN         cargo chef cook --release --recipe-path recipe.json
COPY        . .
RUN         cargo build --release --bin log_processor

FROM        debian:trixie-slim
COPY        --from=builder /etc/apt/sources.list.d/confluent.list /etc/apt/sources.list.d/confluent.list
COPY        --from=builder /usr/share/keyrings/confluent.gpg /usr/share/keyrings/confluent.gpg
RUN         apt-get update && apt-get install -y librdkafka-dev ca-certificates && rm -rf /var/lib/apt/lists/*

RUN         mkdir -p /opt/log_processor
COPY        --from=builder /usr/src/target/release/log_processor /opt/log_processor/
COPY        --from=builder /usr/src/app/log_processor/assets /opt/log_processor/assets

RUN         groupadd --gid 1000 app && useradd --no-create-home --gid app --uid 1000 app
USER        1000

CMD         ["/opt/log_processor/log_processor"]
