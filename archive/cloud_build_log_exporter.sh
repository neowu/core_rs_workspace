#!/bin/bash -ex
repository=$(sed -n 's/^repository = "\(.*\)"/\1/p' build/env.toml | head -1)
version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

gcloud builds submit . --config app/log_exporter/cloudbuild.yml --substitutions=_REPOSITORY=${repository},_VERSION=${version}
