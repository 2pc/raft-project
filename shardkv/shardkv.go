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
type ShardKv struct {
	C          chan InputChannelType
	store      map[string]string
	valid      map[string]bool
	currConfig int64
}

func (s *ShardKv) PeerJoin(ctx context.Context, peer *pb.Peer) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_PeerJoin, Arg: &pb.Command_PeerJoinLeave{PeerJoinLeave: peer}}
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for peer join response")
	result := <-c
	return &result, nil
}

func (s *ShardKv) PeerLeave(ctx context.Context, peer *pb.Peer) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_PeerLeave, Arg: &pb.Command_PeerJoinLeave{PeerJoinLeave: peer}}
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for peer join response")
	result := <-c
	return &result, nil
}

func (s *ShardKv) Get(ctx context.Context, key *pb.Key) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: key}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for get response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *ShardKv) Set(ctx context.Context, in *pb.KeyValue) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for set response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *ShardKv) Clear(ctx context.Context, in *pb.Empty) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for clear response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *ShardKv) CAS(ctx context.Context, in *pb.CASArg) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for CAS response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *ShardKv) KeyMigration(ctx context.Context, in *pb.Reconfig) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{
		Operation: pb.Op_SENDKEY,
		Arg: &pb.Command_KeyMigrate{KeyMigrate: &pb.KeyMigrateArgs{
			Reconfig: in,
			Value:    &pb.Value{Value: ""}, // the value is unused in this case
		}}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for KeyMigration response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

// Used internally to generate a result for a get request. This function assumes that it is called from a single thread of
// execution, and hence does not handle races.
func (s *ShardKv) GetInternal(k string) pb.Result {
	log.Printf("Executing GetInternal, with key: %v", k)
	if !s.valid[k] {
		log.Printf("Currently not responsible for key %v", k)
		return pb.Result{Result: &pb.Result_NotResponsible{NotResponsible: &pb.NotResponsible{}}}
	}
	v := s.store[k]
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally to set and generate an appropriate result. This function assumes that it is called from a single
// thread of execution and hence does not handle race conditions.
func (s *ShardKv) SetInternal(k string, v string) pb.Result {
	log.Printf("Executing SetInternal, with key: %v, value: %v", k, v)
	if !s.valid[k] {
		log.Printf("Currently not responsible for key %v", k)
		return pb.Result{Result: &pb.Result_NotResponsible{NotResponsible: &pb.NotResponsible{}}}
	}
	s.store[k] = v
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally, this function clears a kv store. Assumes no racing calls.
func (s *ShardKv) ClearInternal() pb.Result {
	log.Printf("Executing ClearInternal")
	s.store = make(map[string]string)
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

// Used internally this function performs CAS assuming no races.
func (s *ShardKv) CasInternal(k string, v string, vn string) pb.Result {
	log.Printf("Executing CasInternal, with key: %v, value: %v, v_new: %v", k, v, vn)
	if !s.valid[k] {
		log.Printf("Currently not responsible for key %v", k)
		return pb.Result{Result: &pb.Result_NotResponsible{NotResponsible: &pb.NotResponsible{}}}
	}
	vc := s.store[k]
	if vc == v {
		s.store[k] = vn
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vn}}}
	} else {
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vc}}}
	}
}

func (s *ShardKv) DisableKeyInternal(key string, configId int64) pb.Result {
	log.Printf("Executing DisableKeyInternal, with configId:%v, key:%v", configId, key)
	if configId != s.currConfig+1 {
		log.Printf("Our next config id should be %v, but got %v, ignore", s.currConfig+1, configId)
		return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
	}
	s.valid[key] = false
	s.currConfig = configId
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

func (s *ShardKv) EnableKeyInternal(key string, v string, configId int64) pb.Result {
	log.Printf("Executing EnableKeyInternal, with configId:%v, key:%v, value:%v", configId, key, v)
	if configId != s.currConfig+1 {
		log.Printf("Our next config id should be %v, but got %v, ignore", s.currConfig+1, configId)
		return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
	}
	s.store[key] = v
	s.valid[key] = true
	s.currConfig = configId
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

func (s *ShardKv) UpdateConfigInternal(configId int64) pb.Result {
	log.Printf("Executing UpdateConfigInternal, with configId:%v", configId)
	if configId != s.currConfig+1 {
		log.Printf("Our next config id should be %v, but got %v, ignore", s.currConfig+1, configId)
		return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
	}
	s.currConfig = configId
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

func (s *ShardKv) SendKeyInternal(key string, configId int64) pb.Result {
	log.Printf("Executing UpdateConfigInternal, with configId:%v, key: %v", configId, key)
	if s.currConfig < configId {
		log.Printf("Our configId %v is behind request %v, response with notresponsible", s.currConfig, configId)
		return pb.Result{Result: &pb.Result_NotResponsible{NotResponsible: &pb.NotResponsible{}}}
	}
	value := s.store[key]
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: value}}}
}

// Used internally to dump out all the values
// Could be used as creating snapshot or shard migration
func (s *ShardKv) CreateSnapshot() []byte {
	log.Printf("Dump out all the values from KV store")
	write := new(bytes.Buffer)
	encoder := gob.NewEncoder(write)
	encoder.Encode(s.store)
	snapshot := write.Bytes()
	return snapshot
}

// Used internall to restore from a snapshot
// WARNING: this will force clean the store
func (s *ShardKv) RestoreSnapshot(snapshot []byte) {
	log.Printf("Restore all value from a snapshot")
	read := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(read)
	decoder.Decode(&s.store)
}

func (s *ShardKv) HandleCommand(op InputChannelType) {
	switch c := op.command; c.Operation {
	case pb.Op_GET:
		arg := c.GetGet()
		result := s.GetInternal(arg.Key)
		op.response <- result
	case pb.Op_SET:
		arg := c.GetSet()
		result := s.SetInternal(arg.Key, arg.Value)
		op.response <- result
	case pb.Op_CLEAR:
		result := s.ClearInternal()
		op.response <- result
	case pb.Op_CAS:
		arg := c.GetCas()
		result := s.CasInternal(arg.Kv.Key, arg.Kv.Value, arg.Value.Value)
		op.response <- result
	case pb.Op_PeerJoin:
		result := pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
		op.response <- result
	case pb.Op_PeerLeave:
		result := pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
		op.response <- result
	case pb.Op_DISABLEKEY:
		arg := c.GetKeyMigrate()
		result := s.DisableKeyInternal(arg.Reconfig.Key.Key, arg.Reconfig.ConfigId.ConfigId)
		op.response <- result
	case pb.Op_ENABLEKEY:
		arg := c.GetKeyMigrate()
		result := s.EnableKeyInternal(arg.Reconfig.Key.Key, arg.Value.Value, arg.Reconfig.ConfigId.ConfigId)
		op.response <- result
	case pb.Op_SENDKEY:
		arg := c.GetKeyMigrate()
		result := s.SendKeyInternal(arg.Reconfig.Key.Key, arg.Reconfig.ConfigId.ConfigId)
		op.response <- result
	case pb.Op_UPDATECONFIG:
		arg := c.GetKeyMigrate()
		result := s.UpdateConfigInternal(arg.Reconfig.ConfigId.ConfigId)
		op.response <- result
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		op.response <- pb.Result{}
		log.Fatalf("Unrecognized operation %v", c)
	}
}
