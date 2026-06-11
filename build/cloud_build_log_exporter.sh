#!/bin/bash -ex
app=log_exporter
repository=$(sed -n 's/^repository = "\(.*\)"/\1/p' build/env.toml | head -1)
image=${repository}/${app}
tag=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

gcloud builds submit . --config build/cloudbuild.yml \
  --substitutions=_DOCKERFILE=app/${app}/Dockerfile,_IMAGE=${image},_TAG=${tag},_APP=${app}
