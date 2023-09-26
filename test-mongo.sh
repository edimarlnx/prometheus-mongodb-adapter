#!/usr/bin/env bash
set -e
# create MongoDB container named mongo
docker build -t prometheus-mongodb-adapter .
docker run --rm -it -p 8080:8080 --name prom-adapter \
  --memory 200m --cpus 0.500 --link mongo:mongo \
  -e MONGO_URI="mongodb://mongo:27017/prom-adapter?timeout=5s" \
  prometheus-mongodb-adapter