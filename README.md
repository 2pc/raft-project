Distributed System Class Final Project - Raft Extension
-------------------

## Author

Dayou Du (dayoudu@nyu.edu)

## Description

This repo is the final project in Distributed System Class :D. Basically we added snapshot, sharding, and cluster membership change(peers join/leave) support on our Raft implementation. For the details plase checkout the report.

**For the sharded kv extension, please checkout shardkv branch, which greatly restructed the codes and also re-wrote the helper scripts(though use roughly the same raft implementation).**

## Prerequisities

- A standard linux environment
- Go >= 1.10
- Docker == 18.06.1-ce
- kubernetes

To use the provided launch tool, you will also need:

- Python == 2.7.13

## Get Started

The codes are in `server` directory, while the `client` folder contains some helper testing code. To work with kubernetes, a helper script can be found in `launch-tool/launch.py`, which is initially created by Prof. Aurojit Panda.

### Build/Run Natively

1. `cd server`
2. `go build .`
3. `./server <-peer peer-args>`

To test with the provided client codes:

1. `cd client`
2. `go build .`
3. `./client <dummytest/test/multitest/join/leave> <command args>`

### Build/Run with Kubernetes

For testing you can also use Kubernetes, we have provided a script in `launch-tool/launch.py`. Please not that `launch.py` 
hardcodes a bunch of assumptions about how pods are created, about the fact that we are running under minikube, and that 
the image itself is named `local/raft-peer`. As such one can adopt this script for other purposes, but this will need some work.

To use Kubernetes with this project use `./create-docker-image.sh` to first create a Docker image. Then:

-   `./launch.py boot <num peers>` will boot a cluster with `num peers` participants. Each participant is given a list of
  all other participants (so you can connect to them).
-   `./launch.py list` lists all peers in the current cluster. You can use `kubectl logs <name>` to access the log for a
    particular pod.
-   `./launch.py kill <n>` can be used to kill the nth pod.
-   `./launch.py launch <n>` can be used to relaunch the nth pod (e.g., after it is killed).
-   `./launch.py shutdown` will kill all pods, shutting down the cluster.
-   `./launch.py client-url <n>` can be used to get the URL for the nth pod. One example use of this is `./client
    $(../launch-tool/launch.py client-url 1)` to get a client to connect to pod 1.

## Acknowledgement

The code structure/scripts are based on the [Lab2 starter code](https://github.com/nyu-distributed-systems-fa18/starter-code-lab2), provided by Prof. [Aurojit Panda](https://cs.nyu.edu/~apanda/). All credits goes to him.
