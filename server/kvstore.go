package main

import (
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
type KVStore struct {
	C     chan InputChannelType
	store map[string]string
}

func (s *KVStore) PeerJoin(ctx context.Context, peer *pb.Peer) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_PeerJoin, Arg: &pb.Command_PeerJoinLeave{PeerJoinLeave: peer}}
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for peer join response")
	result := <-c
	return &result, nil
}

func (s *KVStore) PeerLeave(ctx context.Context, peer *pb.Peer) (*pb.Result, error) {
	c := make(chan pb.Result)
	r := pb.Command{Operation: pb.Op_PeerLeave, Arg: &pb.Command_PeerJoinLeave{PeerJoinLeave: peer}}
	s.C <- InputChannelType{command: r, response: c}
	log.Printf("Waiting for peer join response")
	result := <-c
	return &result, nil
}

func (s *KVStore) Get(ctx context.Context, key *pb.Key) (*pb.Result, error) {
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

func (s *KVStore) Set(ctx context.Context, in *pb.KeyValue) (*pb.Result, error) {
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

func (s *KVStore) Clear(ctx context.Context, in *pb.Empty) (*pb.Result, error) {
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

func (s *KVStore) CAS(ctx context.Context, in *pb.CASArg) (*pb.Result, error) {
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

// Used internally to generate a result for a get request. This function assumes that it is called from a single thread of
// execution, and hence does not handle races.
func (s *KVStore) GetInternal(k string) pb.Result {
	log.Printf("Executing GetInternal, with key: %v", k)
	v := s.store[k]
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally to set and generate an appropriate result. This function assumes that it is called from a single
// thread of execution and hence does not handle race conditions.
func (s *KVStore) SetInternal(k string, v string) pb.Result {
	log.Printf("Executing SetInternal, with key: %v, value: %v", k, v)
	s.store[k] = v
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally, this function clears a kv store. Assumes no racing calls.
func (s *KVStore) ClearInternal() pb.Result {
	log.Printf("Executing ClearInternal")
	s.store = make(map[string]string)
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

// Used internally this function performs CAS assuming no races.
func (s *KVStore) CasInternal(k string, v string, vn string) pb.Result {
	log.Printf("Executing CasInternal, with key: %v, value: %v, v_new: %v", k, v, vn)
	vc := s.store[k]
	if vc == v {
		s.store[k] = vn
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vn}}}
	} else {
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vc}}}
	}
}

// Used internally to dump out all the values
// Could be used as creating snapshot or shard migration
func (s *KVStore) DumpInternal() []*pb.KeyValue {
	log.Printf("Dump out all the values from KV store")
	kvdump := make([]*pb.KeyValue, 0)
	for k, v := range s.store {
		kvdump = append(kvdump, &pb.KeyValue{Key: k, Value: v})
	}
	return kvdump
}

// Used internall to restore from a snapshot
// WARNING: this will force clean the store
func (s *KVStore) RestoreSnapshot(kvdump []*pb.KeyValue) {
	log.Printf("Restore all value from a snapshot")
	s.store = make(map[string]string)
	for _, kv := range kvdump {
		s.store[kv.Key] = s.store[kv.Value]
	}
}

func (s *KVStore) HandleCommand(op InputChannelType) {
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
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		op.response <- pb.Result{}
		log.Fatalf("Unrecognized operation %v", c)
	}
}
