FROM        rust:trixie AS builder
RUN         curl https://packages.confluent.io/deb/7.9/archive.key | gpg --dearmor > /usr/share/keyrings/confluent.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/confluent.gpg] https://packages.confluent.io/clients/deb bookworm main" | tee /etc/apt/sources.list.d/confluent.list
RUN         apt-get update && apt-get install -y librdkafka-dev libclang-dev && rm -rf /var/lib/apt/lists/*
WORKDIR     /usr/src
COPY        . .
RUN         cargo install --path app/log_collector

FROM        debian:trixie-slim
COPY        --from=builder /etc/apt/sources.list.d/confluent.list /etc/apt/sources.list.d/confluent.list
COPY        --from=builder /usr/share/keyrings/confluent.gpg /usr/share/keyrings/confluent.gpg
RUN         apt-get update && apt-get install -y librdkafka-dev && rm -rf /var/lib/apt/lists/*

RUN         mkdir -p /opt/log_collector
COPY        --from=builder /usr/local/cargo/bin/log_collector /opt/log_collector/
COPY        --from=builder /usr/src/app/log_collector/assets /opt/log_collector/assets

RUN         groupadd --gid 1000 app && useradd --no-create-home --gid app --uid 1000 app
USER        1000

CMD         ["/opt/log_collector/log_collector"]
