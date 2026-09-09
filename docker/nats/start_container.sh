#!/bin/sh
container run -d --name nats \
    -v ./conf/nats-server.conf:/etc/nats/nats-server.conf:ro \
    nats:2-alpine --config=/etc/nats/nats-server.conf
