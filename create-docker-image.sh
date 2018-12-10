#!/bin/sh
set -E
docker build -t local/shardkv-peer -f Dockerfile_shardkv .
docker build -t local/shardmaster-peer -f Dockerfile_shardmaster .