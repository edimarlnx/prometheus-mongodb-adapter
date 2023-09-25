#!/usr/bin/env bash

docker build -t prometheus-mongodb-adapter .
docker run --rm -it -p 8080:8080 --name prom-adapter \
  --memory 200m --cpus 0.500 \
  -e MONGO_URI="mongodb://localhost:27017/prom-adapter?timeout=5" \
  prometheus-mongodb-adapter