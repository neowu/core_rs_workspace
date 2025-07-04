FROM    rust:1-bookworm AS builder
RUN     curl https://packages.confluent.io/deb/7.9/archive.key | gpg --dearmor > /usr/share/keyrings/confluent.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/confluent.gpg] https://packages.confluent.io/clients/deb bookworm main" | tee /etc/apt/sources.list.d/confluent.list
RUN     apt-get update && apt-get install -y librdkafka-dev libclang-dev && rm -rf /var/lib/apt/lists/*

# install duckdb
RUN     curl -L https://github.com/duckdb/duckdb/releases/download/v1.3.1/duckdb_cli-linux-amd64.zip -o /tmp/duckdb_cli.zip && unzip /tmp/duckdb_cli.zip -d /usr/local/bin && rm /tmp/duckdb_cli.zip
# install gcloud sdk
RUN     curl -SL https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz | tar xzf - -C /opt

WORKDIR /usr/src
COPY    . .
RUN     cargo install --path app/log_exporter

FROM        debian:bookworm-slim
COPY        --from=builder /etc/apt/sources.list.d/confluent.list /etc/apt/sources.list.d/confluent.list
COPY        --from=builder /usr/share/keyrings/confluent.gpg /usr/share/keyrings/confluent.gpg
RUN         apt-get update && apt-get install -y librdkafka-dev python3 && rm -rf /var/lib/apt/lists/*

COPY        --from=builder /usr/local/bin/duckdb /usr/local/bin/duckdb
COPY        --from=builder /opt/google-cloud-sdk /opt/google-cloud-sdk
RUN         ln -s /opt/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud

COPY        --from=builder /usr/local/cargo/bin/log_exporter /usr/local/bin/log_exporter
COPY        --from=builder /usr/src/app/log_exporter/conf/conf.json /usr/local/etc/log_exporter.json

RUN         addgroup --gid 1000 --system app && adduser --system --ingroup app --uid 1000 app
RUN         mkdir -p /var/log/app && chown -R app:app /var/log/app
VOLUME      /var/log/app
USER        1000

ENTRYPOINT  ["log_exporter"]
CMD         ["/usr/local/etc/log_exporter.json"]
