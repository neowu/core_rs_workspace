#!/bin/bash -ex
docker build -f app/log_exporter/Dockerfile -t log_exporter:latest .
