#!/bin/bash -ex
docker build -f app/log_collector/Dockerfile -t log_collector:latest .
