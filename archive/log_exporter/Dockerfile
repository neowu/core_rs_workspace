FROM        rust:trixie AS builder
WORKDIR     /usr/src

RUN         apt-get update && apt-get install -y cmake libclang-dev && rm -rf /var/lib/apt/lists/*
# install duckdb
RUN         curl -L https://github.com/duckdb/duckdb/releases/download/v1.5.2/duckdb_cli-linux-amd64.zip -o /tmp/duckdb_cli.zip && unzip /tmp/duckdb_cli.zip -d /usr/local/bin && rm /tmp/duckdb_cli.zip
# install gcloud sdk
RUN         curl -SL https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz | tar xzf - -C /opt

COPY        . .
RUN         cargo build --release --bin log_exporter

FROM        debian:trixie-slim

COPY        --from=builder /usr/local/bin/duckdb /usr/local/bin/duckdb
COPY        --from=builder /opt/google-cloud-sdk /opt/google-cloud-sdk
RUN         ln -s /opt/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud

RUN         mkdir -p /opt/app
COPY        --from=builder /usr/src/target/release/log_exporter /opt/app/
COPY        --from=builder /usr/src/app/log_exporter/assets /opt/app/assets

# gcloud iam requires user home
RUN         groupadd --gid 1000 app && useradd --create-home --gid app --uid 1000 app
RUN         mkdir -p /var/log/app && chown -R app:app /var/log/app
VOLUME      /var/log/app
USER        1000

CMD         ["/opt/app/log_exporter"]
