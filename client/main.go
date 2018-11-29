package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/raft-extension/pb"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

func CheckConcurrent(total int, c chan int) int {
	count := 0
	for i := 0; i < total; i++ {
		count += (<-c)
	}
	return count
}

func RunConcurrentCAS(endpoint string) {
	log.Printf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")

	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)

	// Clear KVC
	res, err := kvc.Clear(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("Could not clear")
	}

	// Just fire a bunch of CAS operation, where only 1 should be succeed
	putReq := &pb.KeyValue{Key: "x", Value: "y"}
	res, err = kvc.Set(context.Background(), putReq)
	if err != nil {
		log.Fatalf("Put error")
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "x" || res.GetKv().Value != "y" {
		log.Fatalf("Put returned the wrong response")
	}

	total := 100
	c := make(chan int, total)
	for i := 0; i < total; i++ {
		go func(value int) {

			// log.Printf("Connecting to %v", endpoint)
			// Connect to the server. We use WithInsecure since we do not configure https in this class.
			conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
			//Ensure connection did not fail.
			if err != nil {
				log.Printf("Failed to dial GRPC server %v", err)
				return
			}
			// log.Printf("Connected")

			// Create a KvStore client
			kvc := pb.NewKvStoreClient(conn)

			newValue := strconv.Itoa(value)
			// Successfully CAS changing y -> newValue
			casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "x", Value: "y"}, Value: &pb.Value{Value: newValue}}
			res, err = kvc.CAS(context.Background(), casReq)
			if err != nil {
				log.Printf("Request error %v", err)
				return
			}
			// log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
			if res.GetKv().Value == newValue {
				c <- 1
			} else {
				c <- 0
			}
		}(i)
	}

	// Check only one CAS succeed
	if count := CheckConcurrent(total, c); count != 1 {
		log.Fatalf("Expected 1 but got %v", count)
	} else {
		log.Printf("Passed!")
	}
}

func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	endpoint := flag.Args()[0]
	//RunConcurrentCAS(endpoint)
	log.Printf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")
	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)
	// Clear KVC
	res, err := kvc.Clear(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("Could not clear")
	}
	if redirect := res.GetRedirect(); redirect != nil{
		log.Printf("Got redirect response: %v", redirect.Server)
		return
	}

	// Put setting hello -> 1
	putReq := &pb.KeyValue{Key: "hello", Value: "1"}
	res, err = kvc.Set(context.Background(), putReq)
	if err != nil {
		log.Fatalf("Put error")
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Put returned the wrong response")
	}

	// Request value for hello
	req := &pb.Key{Key: "hello"}
	res, err = kvc.Get(context.Background(), req)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Get returned the wrong response")
	}

	// Successfully CAS changing hello -> 2
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "2" {
		log.Fatalf("Get returned the wrong response")
	}

	// Unsuccessfully CAS
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value == "3" {
		log.Fatalf("Get returned the wrong response")
	}

	// CAS should fail for uninitialized variables
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}
}
