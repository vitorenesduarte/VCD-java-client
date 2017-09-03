#!/usr/bin/env bash

DIR=$(dirname "$0")
IMAGE=vitorenesduarte/vcd-java-client
DOCKERFILE=${DIR}/../Dockerfiles/vcd-java-client

# release vcd-java-client
cd ${DIR}/.. && make rel && cd -

# build image
docker build \
  --no-cache \
  -t "${IMAGE}" -f "${DOCKERFILE}" .

# push image
docker push "${IMAGE}"
