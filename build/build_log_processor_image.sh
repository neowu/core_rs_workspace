#!/bin/bash -ex
docker build -f app/log_processor/Dockerfile -t log_processor:latest .
