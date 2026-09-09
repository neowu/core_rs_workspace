#!/bin/sh
container run -d --name postgres \
    -e POSTGRES_PASSWORD=postgres \
    postgres:18
