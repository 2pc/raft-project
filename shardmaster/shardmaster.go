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
	"bytes"
	"encoding/gob"
	"log"

	context "golang.org/x/net/context"

	"github.com/nyu-distributed-systems-fa18/raft-extension/pb"
)

// The struct for data to send over channel
type InputChannelType struct {
	command  pb.Command
	response chan pb.Result
}

// The struct for key value stores.
type ShardMaster struct {
	C         chan InputChannelType
	KeyConfig map[string]int64
	Reconfigs []*pb.Reconfig
}

func (s *ShardMaster) GetKeyGroup(ctx context.Context, key *pb.Key) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: key}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for GetKeyGroup response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *ShardMaster) Reconfig(ctx context.Context, in *pb.ReconfigArgs) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_RECONFIG, Arg: &pb.Command_Reconfig{Reconfig: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for Reconfig response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *ShardMaster) GetReconfig(ctx context.Context, configId *pb.ConfigId) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_GETRECONFIG, Arg: &pb.Command_RetriveReconfig{RetriveReconfig: configId}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for GetReconfig response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *ShardMaster) GetInternal(k string) pb.Result {
	log.Printf("Executing GetInternal, with key: %v", k)
	v := s.KeyConfig[k]
	return pb.Result{Result: &pb.Result_Gid{Gid: &pb.GroupId{Gid: v}}}
}

func (s *ShardMaster) ReconfigInternal(k string, destGid int64) pb.Result {
	log.Printf("Executing ReconfigInternal, with key: %v, destGid: %v", k, destGid)
	if s.KeyConfig[k] == destGid {
		return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
	} else {
		// create a new reconfig entry
		configId := int64(len(s.Reconfigs))
		srcGid := s.KeyConfig[k]

		reconfigItem := &pb.Reconfig{
			ConfigId: &pb.ConfigId{ConfigId: configId},
			Src:      &pb.GroupId{Gid: srcGid},
			Dst:      &pb.GroupId{Gid: destGid},
			Key:      &pb.Key{Key: k},
		}
		log.Printf("Created a new reconfig entry: %v, %v, %v, %v", configId, srcGid, destGid, k)
		s.Reconfigs = append(s.Reconfigs, reconfigItem)
		s.KeyConfig[k] = destGid
		return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
	}
}

func (s *ShardMaster) GetReconfigInternal(configId int64) pb.Result {
	log.Printf("Executing  GetReconfigInternal")
	if configId >= int64(len(s.Reconfigs)) {
		// we don't have more re-configs, simply return success
		return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
	}
	return pb.Result{Result: &pb.Result_Reconfig{Reconfig: s.Reconfigs[configId]}}
}

// Used internally to dump out all the values
// Could be used as creating snapshot or shard migration
func (s *ShardMaster) CreateSnapshot() []byte {
	log.Printf("Dump out all data from shardmaster storage")
	write := new(bytes.Buffer)
	encoder := gob.NewEncoder(write)
	encoder.Encode(s.KeyConfig)
	encoder.Encode(s.Reconfigs)
	snapshot := write.Bytes()
	return snapshot
}

// Used internall to restore from a snapshot
// WARNING: this will force clean the store
func (s *ShardMaster) RestoreSnapshot(snapshot []byte) {
	log.Printf("Restore all value from a snapshot")
	read := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(read)
	decoder.Decode(&s.KeyConfig)
	decoder.Decode(&s.Reconfigs)
}

func (s *ShardMaster) HandleCommand(op InputChannelType) {
	switch c := op.command; c.Operation {
	case pb.Op_GET:
		arg := c.GetGet()
		result := s.GetInternal(arg.Key)
		op.response <- result
	case pb.Op_RECONFIG:
		arg := c.GetReconfig()
		result := s.ReconfigInternal(arg.Key.Key, arg.DestGid.Gid)
		op.response <- result
	case pb.Op_GETRECONFIG:
		arg := c.GetRetriveReconfig()
		result := s.GetReconfigInternal(arg.ConfigId)
		op.response <- result
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		op.response <- pb.Result{}
		log.Fatalf("Unrecognized operation %v", c)
	}
}
