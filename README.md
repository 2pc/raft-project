Distributed System Class Final Project - Raft Extension
-------------------

## Author

Dayou Du (dayoudu@nyu.edu)

## Description

This repo is the final project in Distributed System Class :D. Basically we added snapshot, sharding, and cluster membership change(peers join/leave) support on our Raft implementation. For the details plase checkout the report.

**This shardkv branch is used to implement the sharding extension, which greatly restructed the codes and also re-wrote the helper scripts(though use roughly the same raft implementation).**

## Prerequisities

- A standard linux environment
- Go >= 1.10
- Docker == 18.06.1-ce
- kubernetes

To use the provided launch tool, you will also need:

- Python == 2.7.13

## Get Started

Since we would end up with multiple raft clusters, we assume a fixed naming mechanism (peerX-Y) to mark the peer-group matching. X is the groupID of the peer, and group 0 means this peer belongs to shard master group. Y is the peerID inside each group. e.g: peer0-0 is a shardmaster service peer, while peer2-1 is a shardkv service peer. Thus, 0 is an invalid group assignment of a shard.

The codes are in `shardmaster` and `shardkv` folder respectively. While the `client` folder contains some helper testing code. To work with kubernetes, a helper script can be found in `launch-tool/launch.py`, which is rewritten based on Prof. Aurojit Panda's original script.

### Build/Run Natively

No longer supported

### Build/Run with Kubernetes

We provide a simple script for you to build the whole project. You can just type `built.sh <num groups> <num peers>`. Note that for now these two arguments need to match the settings in the code files. 

For testing you can still use `launch-tool/launch.py`. **Note that the script has been major rewritten to be used in our case, thus the commands/arguments are different with the original one**:

-   `./launch.py boot <num groups> <num peers>` will boot a cluster with `num groups` of shardkv groups (note that we also have a shardmaster group, thus this will result in totally `num gouprs + 1` of raft clusters), while each group has `num peers` participants.
-   `./launch.py list` lists all peers in the current cluster. You can use `kubectl logs <name>` to access the log for a
    particular pod.
-   `./launch.py kill <peer name>` can be used to kill a pod.
-   `./launch.py launch <peer name>` can be used to relaunch a pod (e.g., after it is killed).
-   `./launch.py shutdown` will kill all pods, shutting down the cluster.
-   `./launch.py client-url <peer name>` can be used to get the URL for a pod.

The client codes are also rewritten to be used to test the shardkv system. Generally you can build the client codes in the same way, and run it with `./client <sm/sg> <sm commands/sg commands> <sm command arguments/sg command arguments>`. Please check `client/main.go` for details.

## Acknowledgement

The code structure/scripts are based on the [Lab2 starter code](https://github.com/nyu-distributed-systems-fa18/starter-code-lab2), provided by Prof. [Aurojit Panda](https://cs.nyu.edu/~apanda/). All credits goes to him.
