FROM        rust:trixie AS builder
WORKDIR     /usr/src
COPY        . .
RUN         cargo build --release --bin log_processor_rs

FROM        debian:trixie-slim
RUN         mkdir -p /opt/app
COPY        --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY        --from=builder /usr/src/target/release/log_processor_rs /opt/app/
COPY        --from=builder /usr/src/app/log_processor_rs/assets /opt/app/assets

RUN         groupadd --gid 1000 app && useradd --no-create-home --gid app --uid 1000 app
USER        1000

CMD         ["/opt/app/log_processor_rs"]
