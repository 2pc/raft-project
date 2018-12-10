#!/bin/sh
set -E
cd pb
go fmt
protoc --go_out=plugins=grpc:. kv.proto

cd ../shardmaster
go fmt
go get -v ./...
go build .

#cd ../shardkv
#go fmt
#go get -v ./...
#go build .

#cd ../sharded-raft-test-client
#go fmt
#go get -v ./...
#go build .

cd ..
./create-docker-image.sh

launch-tool/launch.py shutdown
launch-tool/launch.py list
sleep 10 #sleep 10 sec to let the shard master server to be stable
launch-tool/launch.py boot 0 5 #start kv group 0
launch-tool/launch.py list