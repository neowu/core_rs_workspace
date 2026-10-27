#!/bin/sh
container run -d --name iggy \
  --cap-add=SYS_NICE --ulimit memlock=-1:-1 \
  -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
  -e IGGY_TCP_ADDRESS=0.0.0.0:8090 -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
  -e IGGY_NODE_ADVERTISED_ADDRESS=iggy.test \
  -p 8090:8090 -p 3000:3000 \
  apache/iggy
