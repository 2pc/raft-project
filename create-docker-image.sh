#!/bin/sh
set -E
#docker build -t local/shardkv-peer -f Dockerfile_shardedkv .
docker build -t local/shardmaster-peer -f Dockerfile_shardmaster .