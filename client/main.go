package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/raft-extension/pb"
)

const (
	NUM_PEER_IN_GROUP    = 5
	NUM_GROUP_IN_CLUSTER = 3
)

func usage() {
	fmt.Printf("Usage %s <sm/sg> <command> <args>\n", os.Args[0])
	flag.PrintDefaults()
}

func peer_name_split(peer string) (int64, int64) {
	tmp := strings.Split(peer, "-")
	group_id, _ := strconv.Atoi(tmp[0][4:])
	peer_id, _ := strconv.Atoi(tmp[1])
	return int64(group_id), int64(peer_id)
}

func getKVServiceURL(peer string) (string, error) {
	cmd := exec.Command("../launch-tool/launch.py", "client-url", peer)
	stdout, err := cmd.Output()
	if err != nil {
		return "", err
	}
	endpoint := strings.Trim(string(stdout), "\n")
	return endpoint, nil
}

func connToEndpoint(endpoint string) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithTimeout(3000*time.Millisecond))
	return conn, err
}

func getServerAtNextIndex(groupId int64, serverIndex *int) string {
	peerName := fmt.Sprintf("peer%d-%d", groupId, *serverIndex)
	endpoint, err := getKVServiceURL(peerName)
	*serverIndex += 1
	if *serverIndex >= NUM_PEER_IN_GROUP {
		*serverIndex = 0
	}
	if err != nil {
		return getServerAtNextIndex(groupId, serverIndex)
	}
	return endpoint
}

func getShardKvAtNextIndex(groupId int64, serverIndex *int) pb.ShardKvClient {
	endpoint := getServerAtNextIndex(groupId, serverIndex)
	conn, err := connToEndpoint(endpoint)
	if err != nil {
		*serverIndex += 1
		if *serverIndex >= NUM_PEER_IN_GROUP {
			*serverIndex = 0
		}
		return getShardKvAtNextIndex(groupId, serverIndex)
	}
	// Create a KvStore client
	kvc := pb.NewShardKvClient(conn)
	return kvc
}

func getShardMasterAtNextIndex(serverIndex *int) pb.ShardMasterClient {
	endpoint := getServerAtNextIndex(0, serverIndex)
	conn, err := connToEndpoint(endpoint)
	if err != nil {
		*serverIndex += 1
		if *serverIndex >= NUM_PEER_IN_GROUP {
			*serverIndex = 0
		}
		return getShardMasterAtNextIndex(serverIndex)
	}
	smc := pb.NewShardMasterClient(conn)
	return smc
}

func getShardKvAtRedirect(peer string, serverIndex *int) pb.ShardKvClient {
	groupId, _ := peer_name_split(peer)
	endpoint, err := getKVServiceURL(peer)
	if err != nil {
		return getShardKvAtNextIndex(groupId, serverIndex)
	}
	conn, err := connToEndpoint(endpoint)
	if err != nil {
		return getShardKvAtNextIndex(groupId, serverIndex)
	}
	// Create a KvStore client
	kvc := pb.NewShardKvClient(conn)
	return kvc
}

func getShardMasterAtRedirect(peer string, serverIndex *int) pb.ShardMasterClient {
	endpoint, err := getKVServiceURL(peer)
	if err != nil {
		return getShardMasterAtNextIndex(serverIndex)
	}
	conn, err := connToEndpoint(endpoint)
	if err != nil {
		return getShardMasterAtNextIndex(serverIndex)
	}
	// Create a KvStore client
	smc := pb.NewShardMasterClient(conn)
	return smc
}

func sg_single_checker(key string) {
	serverIndex := 0
	flag := false

	// fisrt, get group id
	keyGid := int64(0)
	for keyGid == 0 {
		time.Sleep(200 * time.Millisecond)
		keyGid = smGetConfig(key)
	}
	kvc := getShardKvAtNextIndex(keyGid, &serverIndex)
	for {
		flag = true
		for flag {
			// Put setting key -> 1
			putReq := &pb.KeyValue{Key: key, Value: "1"}
			ctx, cancel := context.WithTimeout(context.Background(), 3000*time.Millisecond)
			res, err := kvc.Set(ctx, putReq)
			if err != nil {
				kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
				continue
			}
			cancel()
			if redirect := res.GetRedirect(); redirect != nil {
				log.Printf("Got redirect response: %v", redirect.Server)
				if redirect.Server != "" {
					kvc = getShardKvAtRedirect(redirect.Server, &serverIndex)
				} else {
					kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
				}
				continue
			}
			if notResponsible := res.GetNotResponsible(); notResponsible != nil {
				log.Printf("Got not responsible response, query ShardMaster for new key assignment")
				keyGid = smGetConfig(key)
				for keyGid == 0 {
					time.Sleep(200 * time.Millisecond)
					keyGid = smGetConfig(key)
				}
				kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
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
			ctx, cancel := context.WithTimeout(context.Background(), 3000*time.Millisecond)
			res, err := kvc.Get(ctx, req)
			if err != nil {
				kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
				continue
			}
			cancel()
			if redirect := res.GetRedirect(); redirect != nil {
				log.Printf("Got redirect response: %v", redirect.Server)
				if redirect.Server != "" {
					kvc = getShardKvAtRedirect(redirect.Server, &serverIndex)
				} else {
					kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
				}
				continue
			}
			if notResponsible := res.GetNotResponsible(); notResponsible != nil {
				log.Printf("Got not responsible response, query ShardMaster for new key assignment")
				keyGid = smGetConfig(key)
				for keyGid == 0 {
					time.Sleep(200 * time.Millisecond)
					keyGid = smGetConfig(key)
				}
				kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
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
			ctx, cancel := context.WithTimeout(context.Background(), 3000*time.Millisecond)
			res, err := kvc.CAS(ctx, casReq)
			if err != nil {
				kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
				continue
			}
			cancel()
			if redirect := res.GetRedirect(); redirect != nil {
				log.Printf("Got redirect response: %v", redirect.Server)
				if redirect.Server != "" {
					kvc = getShardKvAtRedirect(redirect.Server, &serverIndex)
				} else {
					kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
				}
				continue
			}
			if notResponsible := res.GetNotResponsible(); notResponsible != nil {
				log.Printf("Got not responsible response, query ShardMaster for new key assignment")
				keyGid = smGetConfig(key)
				for keyGid == 0 {
					time.Sleep(200 * time.Millisecond)
					keyGid = smGetConfig(key)
				}
				kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
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
			ctx, cancel := context.WithTimeout(context.Background(), 3000*time.Millisecond)
			res, err := kvc.CAS(ctx, casReq)
			if err != nil {
				kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
				continue
			}
			cancel()
			if redirect := res.GetRedirect(); redirect != nil {
				log.Printf("Got redirect response: %v", redirect.Server)
				if redirect.Server != "" {
					kvc = getShardKvAtRedirect(redirect.Server, &serverIndex)
				} else {
					kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
				}
				continue
			}
			if notResponsible := res.GetNotResponsible(); notResponsible != nil {
				log.Printf("Got not responsible response, query ShardMaster for new key assignment")
				keyGid = smGetConfig(key)
				for keyGid == 0 {
					time.Sleep(200 * time.Millisecond)
					keyGid = smGetConfig(key)
				}
				kvc = getShardKvAtNextIndex(keyGid, &serverIndex)
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

func sg_dummytest(peer string) {
	log.Printf("Connecting to %v", peer)

	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	endpoint, err := getKVServiceURL(peer)
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	conn, err := connToEndpoint(endpoint)

	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")

	// Create a KvStore client
	kvc := pb.NewShardKvClient(conn)

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
	if nr := res.GetNotResponsible(); nr != nil {
		log.Printf("Got not responsible")
		return
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
	if nr := res.GetNotResponsible(); nr != nil {
		log.Printf("Got not responsible")
		return
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
	if nr := res.GetNotResponsible(); nr != nil {
		log.Printf("Got not responsible")
		return
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
	if nr := res.GetNotResponsible(); nr != nil {
		log.Printf("Got not responsible")
		return
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
	if nr := res.GetNotResponsible(); nr != nil {
		log.Printf("Got not responsible")
		return
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}
}

func smGetConfig(key string) int64 {
	serverIndex := 0
	smc := getShardMasterAtNextIndex(&serverIndex)
	for true {
		res, err := smc.GetKeyGroup(context.Background(), &pb.Key{Key: key})
		if err != nil {
			log.Printf("%v", err)
			smc = getShardMasterAtNextIndex(&serverIndex)
		}
		if redirect := res.GetRedirect(); redirect != nil {
			log.Printf("Got redirect response: %v", redirect.Server)
			smc = getShardMasterAtRedirect(redirect.Server, &serverIndex)
			continue
		}
		log.Printf("Got response GroupId:%v", res.GetGid().Gid)
		return res.GetGid().Gid
	}
	return 0
}

func smSetConfig(key string, dstGid int64) {
	serverIndex := 0
	smc := getShardMasterAtNextIndex(&serverIndex)
	for true {
		smSetReq := &pb.ReconfigArgs{Key: &pb.Key{Key: key}, DestGid: &pb.GroupId{Gid: dstGid}}
		res, err := smc.Reconfig(context.Background(), smSetReq)
		if err != nil {
			log.Printf("%v", err)
			smc = getShardMasterAtNextIndex(&serverIndex)
		}
		if redirect := res.GetRedirect(); redirect != nil {
			log.Printf("Got redirect response: %v", redirect.Server)
			smc = getShardMasterAtRedirect(redirect.Server, &serverIndex)
			continue
		}
		log.Printf("Set complete")
		return
	}
}

func smGetReconfig(configId int64) {
	serverIndex := 0
	flag := true
	smc := getShardMasterAtNextIndex(&serverIndex)
	for flag {
		smGetReconfigReq := &pb.ConfigId{ConfigId: configId}
		res, err := smc.GetReconfig(context.Background(), smGetReconfigReq)
		if err != nil {
			log.Printf("%v", err)
			smc = getShardMasterAtNextIndex(&serverIndex)
		}
		if redirect := res.GetRedirect(); redirect != nil {
			log.Printf("Got redirect response: %v", redirect.Server)
			smc = getShardMasterAtRedirect(redirect.Server, &serverIndex)
			continue
		}
		reconfig := res.GetReconfig()
		log.Printf("Got response configId:%v, src group:%v, dst group:%v, key: %v",
			reconfig.ConfigId.ConfigId, reconfig.Src.Gid, reconfig.Dst.Gid, reconfig.Key.Key)
		return
	}
}

func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no option fail
	if flag.NArg() <= 2 {
		flag.Usage()
		os.Exit(1)
	}
	optype := flag.Args()[0]
	switch optype {
	case "sm":
		command := flag.Args()[1]
		switch command {
		case "getconfig":
			key := flag.Args()[2]
			smGetConfig(key)
		case "setconfig":
			key := flag.Args()[2]
			parseInt, _ := strconv.Atoi(flag.Args()[3])
			dstGroup := int64(parseInt)
			smSetConfig(key, dstGroup)
		case "getreconfig":
			parseInt, _ := strconv.Atoi(flag.Args()[2])
			configId := int64(parseInt)
			smGetReconfig(configId)
		}
	case "sg":
		command := flag.Args()[1]
		switch command {
		case "dummytest":
			endpoint := flag.Args()[2]
			sg_dummytest(endpoint)
		case "test":
			key := flag.Args()[2]
			sg_single_checker(key)
		}

	default:
		flag.Usage()
		os.Exit(1)
	}
}
