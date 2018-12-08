package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"regexp"
	// "strconv"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/raft-extension/pb"
)

func usage() {
	fmt.Printf("Usage %s <dummytest/test/multitest/join/leave>\n", os.Args[0])
	flag.PrintDefaults()
}

func getKVServiceURL(peer string) string {
	cmd := exec.Command("../launch-tool/launch.py", "client-url", peer)
	stdout, err := cmd.Output()
	if err != nil {
		log.Fatalf("Cannot get the service URL.peer: %v, err:%v", peer, err)
	}
	endpoint := strings.Trim(string(stdout), "\n")
	return endpoint
}

func listAvailRaftServer() []string {
	cmd := exec.Command("../launch-tool/launch.py", "list")
	stdout, err := cmd.Output()
	if err != nil {
		log.Fatalf("Cannot list Raft servers.")
	}
	re := regexp.MustCompile("peer[0-9]+")
	peers := re.FindAllString(string(stdout), -1)
	return peers
}

func getServerAtNextIndex(allServer []string, serverIndex *int) string {
	if *serverIndex > len(allServer) {
		*serverIndex = 0
	}
	return getKVServiceURL(allServer[*serverIndex])
}

func getKvcAtNextIndex(allServer []string, serverIndex *int) pb.KvStoreClient {
	endpoint := getServerAtNextIndex(allServer, serverIndex)
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		return getKvcAtNextIndex(allServer, serverIndex)
	}
	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)
	return kvc
}

func getKvcAtRedirect(endpoint string, allServer []string, serverIndex *int) pb.KvStoreClient {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	if err != nil {
		return getKvcAtNextIndex(allServer, serverIndex)
	}
	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)
	return kvc
}

func single_checker(key string, allServer []string) {
	serverIndex := 0
	kvc := getKvcAtNextIndex(allServer, &serverIndex)
	flag := false
	for {
		flag = true
		for flag {
			// Put setting key -> 1
			putReq := &pb.KeyValue{Key: key, Value: "1"}
			res, err := kvc.Set(context.Background(), putReq)
			if err != nil {
				kvc = getKvcAtNextIndex(allServer, &serverIndex)
				continue
			}
			if redirect := res.GetRedirect(); redirect != nil {
				log.Printf("Got redirect response: %v", redirect.Server)
				kvc = getKvcAtRedirect(redirect.Server, allServer, &serverIndex)
				continue
			}
			log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
			if res.GetKv().Key != key || res.GetKv().Value != "1" {
				log.Fatalf("Put returned the wrong response")
			}
			flag = false
		}

		flag = true
		for flag {
			// Request value for key
			req := &pb.Key{Key: key}
			res, err := kvc.Get(context.Background(), req)
			if err != nil {
				kvc = getKvcAtNextIndex(allServer, &serverIndex)
				continue
			}
			if redirect := res.GetRedirect(); redirect != nil {
				log.Printf("Got redirect response: %v", redirect.Server)
				kvc = getKvcAtRedirect(redirect.Server, allServer, &serverIndex)
				continue
			}
			log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
			if res.GetKv().Key != key || res.GetKv().Value != "1" {
				log.Fatalf("Get returned the wrong response")
			}
			flag = false
		}

		flag = true
		for flag {
			// Successfully CAS changing key -> 2
			casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: key, Value: "1"}, Value: &pb.Value{Value: "2"}}
			res, err := kvc.CAS(context.Background(), casReq)
			if err != nil {
				kvc = getKvcAtNextIndex(allServer, &serverIndex)
				continue
			}
			if redirect := res.GetRedirect(); redirect != nil {
				log.Printf("Got redirect response: %v", redirect.Server)
				kvc = getKvcAtRedirect(redirect.Server, allServer, &serverIndex)
				continue
			}
			log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
			if res.GetKv().Key != key || res.GetKv().Value != "2" {
				log.Fatalf("CAS returned the wrong response")
			}
			flag = false
		}

		flag = true
		for flag {
			// Unsuccessfully CAS
			casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: key, Value: "1"}, Value: &pb.Value{Value: "3"}}
			res, err := kvc.CAS(context.Background(), casReq)
			if err != nil {
				kvc = getKvcAtNextIndex(allServer, &serverIndex)
				continue
			}
			if redirect := res.GetRedirect(); redirect != nil {
				log.Printf("Got redirect response: %v", redirect.Server)
				kvc = getKvcAtRedirect(redirect.Server, allServer, &serverIndex)
				continue
			}
			log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
			if res.GetKv().Key != key || res.GetKv().Value == "3" {
				log.Fatalf("CAS returned the wrong response")
			}
			flag = false
		}
	}
}

func dummytest(endpoint string) {
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
	if redirect := res.GetRedirect(); redirect != nil {
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

func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no option fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	optype := flag.Args()[0]
	switch optype {
	case "dummytest":
		endpoint := flag.Args()[1]
		dummytest(endpoint)
	case "test":
		single_checker("hello", listAvailRaftServer())
	default:
		flag.Usage()
		os.Exit(1)
	}
}
