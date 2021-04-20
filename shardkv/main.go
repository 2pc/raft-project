/**
   Copyright 2019 Dayou Du

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
**/

package main

import (
	"flag"
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	pb "../pb"
	//"github.com/nyu-distributed-systems-fa18/raft-extension/pb"
)

const (
	NUM_PEER_IN_GROUP    = 5
	NUM_GROUP_IN_CLUSTER = 3
)

func peer_name_split(peer string) (int, int) {
	tmp := strings.Split(peer, "-")
	group_id, _ := strconv.Atoi(tmp[0][4:])
	peer_id, _ := strconv.Atoi(tmp[1])
	return group_id, peer_id
}

func main() {
	// Argument parsing
	var r *rand.Rand
	var seed int64
	var clientPort int
	var raftPort int
	flag.Int64Var(&seed, "seed", -1,
		"Seed for random number generator, values less than 0 result in use of time")
	flag.IntVar(&clientPort, "port", 3000,
		"Port on which server should listen to client requests")
	flag.IntVar(&raftPort, "raft", 3001,
		"Port on which server should listen to Raft requests")
	flag.Parse()

	// Initialize the random number generator
	if seed < 0 {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	} else {
		r = rand.New(rand.NewSource(seed))
	}

	// Get hostname
	name, err := os.Hostname()
	if err != nil {
		// Without a host name we can't really get an ID, so die.
		log.Fatalf("Could not get hostname")
	}

	id := fmt.Sprintf("%s:%d", name, raftPort)
	log.Printf("Starting peer with ID %s", id)

	// Convert port to a string form
	portString := fmt.Sprintf(":%d", clientPort)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	// construct the peers in my group
	group_id, _ := peer_name_split(name)
	peers := make([]string, 0)
	for i := 0; i < NUM_PEER_IN_GROUP; i++ {
		peerString := fmt.Sprintf("peer%d-%d:%d", group_id, i, raftPort)
		if id != peerString {
			peers = append(peers, peerString)
		}
	}

	// construct the peers in shardmaster/other groups
	services := make(map[int64]([]string))
	for i := 0; i <= NUM_GROUP_IN_CLUSTER; i++ {
		if i != group_id {
			services[int64(i)] = make([]string, 0)
			for j := 0; j < NUM_PEER_IN_GROUP; j++ {
				peerString := fmt.Sprintf("peer%d-%d:%d", i, j, clientPort)
				services[int64(i)] = append(services[int64(i)], peerString)
			}
		}
	}

	// Initialize KVStore
	store := ShardKv{
		C:          make(chan InputChannelType),
		store:      make(map[string]string),
		valid:      make(map[string]bool),
		currConfig: int64(-1),
	}
	go serve(&store, r, &peers, services, id, int64(group_id), raftPort)

	// Tell GRPC that s will be serving requests for the KvStore service and should use store (defined on line 23)
	// as the struct whose methods should be called in response.
	pb.RegisterShardKvServer(s, &store)
	log.Printf("Going to listen on port %v", clientPort)
	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
	log.Printf("Done listening")
}
