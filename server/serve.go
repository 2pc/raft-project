package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"strings"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/raft-extension/pb"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

type InstallSnapshotInput struct {
	arg      *pb.InstallSnapshotArgs
	response chan pb.InstallSnapshotRet
}

type AppendResponse struct {
	ret  *pb.AppendEntriesRet
	arg  *pb.AppendEntriesArgs
	err  error
	peer string
}

type VoteResponse struct {
	ret  *pb.RequestVoteRet
	arg  *pb.RequestVoteArgs
	err  error
	peer string
}

type InstallShapshotResponse struct {
	ret  *pb.InstallSnapshotRet
	arg  *pb.InstallSnapshotArgs
	err  error
	peer string
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	id string
	// ---------------------- helper variables --------------------------------------------
	AppendChan          chan AppendEntriesInput
	VoteChan            chan VoteInput
	InstallSnapshotChan chan InstallSnapshotInput

	state      int // 0:follower 1:candidate 2:leader
	voteCount  int // count of votes we have got
	numPeers   int // total number of peers in the system
	majorCount int // = numPeers/2 + 1

	// ---------------------- non-volatile on each server ---------------------------------
	// but - in our homeworks we don't really have a stable storage
	currentTerm   int64
	votedFor      string
	log           []*pb.Entry
	firstLogIndex int64
	lastLogIndex  int64
	lastLogTerm   int64

	// ---------------------- valatile on each server -------------------------------------
	commitIndex int64
	lastApplied int64

	// snapshot related
	snapshotData      []*pb.KeyValue
	lastIncludedIndex int64
	lastIncludedTerm  int64

	// each time we reach this limit, we will compact current COMMIT POINT state into the snapshot
	maxCommitLog int64

	// ----------------------- volatile on the leaders ------------------------------------
	// Should be re-initilize after election

	// 1. index of the next log entry to send to that server
	// initialized to leader last log index + 1)
	nextIndex map[string]int64

	// 2. index of the highest log entry known to be replicated on server
	// initialized to -1, increase monotonically
	matchIndex map[string]int64

	// 3. response channels - temporally save the response channel until we can actually deal with them
	// mapping from a log entry to a chan pb.Result
	responseChans map[int64]*chan pb.Result
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) InstallSnapshot(ctx context.Context, arg *pb.InstallSnapshotArgs) (*pb.InstallSnapshotRet, error) {
	c := make(chan pb.InstallSnapshotRet)
	r.InstallSnapshotChan <- InstallSnapshotInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	const DurationMax = 20000
	const DurationMin = 5000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand, isHeartbeat bool) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}
	}

	if isHeartbeat {
		timer.Reset(1000 * time.Millisecond)
	} else {
		timer.Reset(randomDuration(r))
	}
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

func createSnapShot(r *Raft, s *KVStore) {
	// pull out all data, we don't have a disk, thus we just store them in memory
	r.snapshotData = s.DumpInternal()
	r.lastIncludedIndex = r.lastApplied
	r.lastIncludedTerm = r.log[r.lastApplied-r.firstLogIndex].Term

	// now, we discard all the executed logs so far.
	// WARNING: CARFULLY CHECK THE INDEX(-1, +1) HERE
	r.log = r.log[(r.lastApplied - r.firstLogIndex + 1):]
	r.firstLogIndex = r.lastApplied + 1

	log.Printf("Snapshot created, last index:%v, last term:%v", r.lastIncludedIndex, r.lastIncludedTerm)
}

func sendSnapShot(raft *Raft, p string, c pb.RaftClient, installSnapshotResponseChan *chan InstallShapshotResponse) {
	log.Printf("Sending peer %v snap shot, last index:%v, last term:%v", p, raft.lastIncludedIndex, raft.lastIncludedTerm)
	// send my snapshot to the peer
	args := pb.InstallSnapshotArgs{
		Term:              raft.currentTerm,
		LeaderID:          raft.id,
		LastIncludedIndex: raft.lastIncludedIndex,
		LastIncludedTerm:  raft.lastIncludedTerm,
		Data:              raft.snapshotData,
	}
	go func(c pb.RaftClient,
		p string,
		args *pb.InstallSnapshotArgs) {
		ret, err := c.InstallSnapshot(context.Background(), args)
		*installSnapshotResponseChan <- InstallShapshotResponse{ret: ret, arg: args, err: err, peer: p}
	}(c, p, &args)
}

func broadcastHeartbeat(raft *Raft, peerClients *map[string]pb.RaftClient,
	appendResponseChan *chan AppendResponse, installSnapshotResponseChan *chan InstallShapshotResponse) {
	for p, c := range *peerClients {
		prevLogIndex := raft.nextIndex[p] - 1

		// This means instead of a heartbeat, we need to install a snapshot
		if prevLogIndex < raft.lastIncludedIndex {
			sendSnapShot(raft, p, c, installSnapshotResponseChan)
			continue
		}
		var prevLogTerm int64 = -1
		if prevLogIndex == raft.lastIncludedIndex {
			prevLogTerm = raft.lastIncludedTerm
		} else {
			prevLogTerm = raft.log[prevLogIndex-raft.firstLogIndex].Term
		}
		args := pb.AppendEntriesArgs{
			Term:         raft.currentTerm,
			LeaderID:     raft.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      make([]*pb.Entry, 0),
			LeaderCommit: raft.commitIndex,
		}
		go func(c pb.RaftClient,
			p string,
			args *pb.AppendEntriesArgs) {
			ret, err := c.AppendEntries(context.Background(), args)
			*appendResponseChan <- AppendResponse{ret: ret, arg: args, err: err, peer: p}
		}(c, p, &args)
	}
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	raft := Raft{
		id: id,

		AppendChan:          make(chan AppendEntriesInput),
		VoteChan:            make(chan VoteInput),
		InstallSnapshotChan: make(chan InstallSnapshotInput),

		state:      0,
		voteCount:  0,
		numPeers:   len(*peers),
		majorCount: (len(*peers)+1)/2 + 1,

		currentTerm:   -1,
		votedFor:      "",
		log:           make([]*pb.Entry, 0),
		firstLogIndex: 0,
		lastLogIndex:  -1,
		lastLogTerm:   -1,

		commitIndex: -1,
		lastApplied: -1,

		snapshotData:      make([]*pb.KeyValue, 0),
		lastIncludedIndex: -1,
		lastIncludedTerm:  -1,
		maxCommitLog:      5, // temporally we set a quite small value to see the effect :)

		nextIndex:     nil,
		matchIndex:    nil,
		responseChans: nil, // only a leader needs to response to clients
	}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)
	installSnapshotResponseChan := make(chan InstallShapshotResponse)

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))

	// Run forever handling inputs from various channels
	for {
		select {
		case <-timer.C:
			// The timer went off.
			if raft.state == 0 || raft.state == 1 {
				log.Printf("Timeout, start a new round of election with term %v", raft.currentTerm+1)

				// reset raft states
				raft.state = 1
				raft.voteCount = 1 // yes we already voted for ourselves
				raft.votedFor = id

				// increase the current term
				raft.currentTerm += 1

				for p, c := range peerClients {
					// Send in parallel so we don't wait for each client.
					args := pb.RequestVoteArgs{
						Term:         raft.currentTerm,
						CandidateID:  id,
						LastLogIndex: raft.lastLogIndex,
						LasLogTerm:   raft.lastLogTerm,
					}
					go func(c pb.RaftClient, p string, args *pb.RequestVoteArgs) {
						ret, err := c.RequestVote(context.Background(), args)
						voteResponseChan <- VoteResponse{ret: ret, arg: args, err: err, peer: p}
					}(c, p, &args)
				}
				// This will also take care of any pesky timeouts that happened while processing the operation.
				restartTimer(timer, r, false)
			} else {
				log.Printf("Leader trigger a new round of heatbeat messages with term %v", raft.currentTerm)
				broadcastHeartbeat(&raft, &peerClients, &appendResponseChan, &installSnapshotResponseChan)

				// This would be a heartbeat timer
				restartTimer(timer, r, true)
			}
		case op := <-s.C:
			if raft.state == 0 {
				// as a follower, we should response with redirect message
				server := raft.votedFor
				if server == "" {
					// this means we are not sure who's the leader yet.
				} else {
					server = strings.Split(server, ":")[0]
				}
				op.response <- pb.Result{
					Result: &pb.Result_Redirect{
						Redirect: &pb.Redirect{
							Server: server,
						},
					},
				}
			} else if raft.state == 2 {
				// as a leader, we should add this to our log
				// and also save the response channel
				newEntry := pb.Entry{
					Term:  raft.currentTerm,
					Index: raft.lastLogIndex + 1,
					Cmd:   &op.command,
				}
				raft.log = append(raft.log, &newEntry)
				raft.lastLogIndex++
				raft.lastLogTerm = raft.currentTerm
				raft.responseChans[raft.lastLogIndex] = &op.response
			} else {
				// as a candidate, we really don't know what to do.......
				server := ""
				op.response <- pb.Result{
					Result: &pb.Result_Redirect{
						Redirect: &pb.Redirect{
							Server: server,
						},
					},
				}
			}
		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer
			log.Printf("Received append entry from %v, term %v", ae.arg.LeaderID, ae.arg.Term)

			if ae.arg.Term < raft.currentTerm {
				log.Printf("Reject append entry from %v, due to arg.Term %v smaller than term %v",
					ae.arg.LeaderID, ae.arg.Term, raft.currentTerm)
				ae.response <- pb.AppendEntriesRet{Term: raft.currentTerm, Success: false}
				break
			}

			if ae.arg.Term >= raft.currentTerm {
				// Transit to follower
				raft.state = 0
				raft.voteCount = 0
				raft.currentTerm = ae.arg.Term
				raft.votedFor = ae.arg.LeaderID
			}

			if (ae.arg.PrevLogIndex > raft.lastLogIndex) ||
				((ae.arg.PrevLogIndex >= raft.firstLogIndex) && (ae.arg.PrevLogTerm != raft.log[ae.arg.PrevLogIndex-raft.firstLogIndex].Term)) {
				log.Printf("Reject append entry from %v, due to prevLogIndex too large or prevLogTerm mismatch", ae.arg.LeaderID)
				ae.response <- pb.AppendEntriesRet{Term: raft.currentTerm, Success: false}
				break
			}

			startIndex := ae.arg.PrevLogIndex + 1
			for id, val := range ae.arg.Entries {
				appendingId := startIndex + int64(id)
				log.Printf("Try to append log entry index id: %v", appendingId)

				// we don't need to care about already committed entries
				if appendingId <= raft.commitIndex {
					continue
				}

				if appendingId > raft.lastLogIndex {
					raft.log = append(raft.log, val)
					raft.lastLogIndex = appendingId
				} else {
					if raft.log[appendingId-raft.firstLogIndex].Term != val.Term {
						// term mismatch, delete all entries that follows it
						log.Printf("Term mismatch, delete all entries starting from %v", appendingId)
						raft.log = raft.log[0:(appendingId - raft.firstLogIndex)]
						raft.log = append(raft.log, val)
						raft.lastLogIndex = appendingId
					}
				}
			}

			if raft.lastLogIndex >= raft.firstLogIndex {
				raft.lastLogTerm = raft.log[raft.lastLogIndex-raft.firstLogIndex].Term
			}
			raft.commitIndex = max(raft.commitIndex, min(ae.arg.LeaderCommit, raft.lastLogIndex))

			// run the commands in our local kvstore
			for nextApply := raft.lastApplied + 1; nextApply <= raft.commitIndex; nextApply++ {
				log.Printf("Try to execute log entry index id: %v", nextApply)
				c := make(chan pb.Result)
				arg := InputChannelType{
					command:  *raft.log[nextApply-raft.firstLogIndex].Cmd,
					response: c,
				}
				go func(parg *InputChannelType) {
					s.HandleCommand(*parg)
				}(&arg)
				<-c
				raft.lastApplied = nextApply
			}

			// if we have enough executed commands, we create a snapshot
			if raft.lastApplied-raft.firstLogIndex+1 > raft.maxCommitLog {
				createSnapShot(&raft, s)
			}

			ae.response <- pb.AppendEntriesRet{Term: raft.currentTerm, Success: true}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r, false)

		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			log.Printf("Received vote request from %v, term %v", vr.arg.CandidateID, vr.arg.Term)

			// update the term we have seen
			if vr.arg.Term > raft.currentTerm {
				// reset as a follower
				log.Printf("Term %v greater than currect term %v, reset as a follower", vr.arg.Term, raft.currentTerm)
				raft.currentTerm = vr.arg.Term
				raft.votedFor = ""
				raft.state = 0
				// restartTimer(timer, r, false)
			}

			if raft.state != 0 {
				log.Printf("Reject %v 's vote request - only followers can vote", vr.arg.CandidateID)
				vr.response <- pb.RequestVoteRet{Term: raft.currentTerm, VoteGranted: false}
				break
			}

			if vr.arg.Term >= raft.currentTerm {
				// When should we accept a vote? safty requirements + not accept from others
				if (vr.arg.LasLogTerm >= raft.lastLogTerm) ||
					((vr.arg.LasLogTerm == raft.lastLogTerm) && (vr.arg.LastLogIndex >= raft.lastLogIndex)) {
					if (raft.votedFor == "") || (raft.votedFor == vr.arg.CandidateID) {
						log.Printf("Accpect %v 's vote request", vr.arg.CandidateID)

						// set granted so that no others could get vote in this term
						raft.votedFor = vr.arg.CandidateID
						vr.response <- pb.RequestVoteRet{Term: raft.currentTerm, VoteGranted: true}

						// reset timer
						restartTimer(timer, r, false)
					} else {
						log.Printf("Reject %v 's vote request - already granted to others", vr.arg.CandidateID)
						vr.response <- pb.RequestVoteRet{Term: raft.currentTerm, VoteGranted: false}
					}
				} else {
					log.Printf("Reject %v 's vote request - not meeting safty requirements", vr.arg.CandidateID)
					vr.response <- pb.RequestVoteRet{Term: raft.currentTerm, VoteGranted: false}
				}
			} else {
				log.Printf("Reject %v 's vote request - arg.term %v less than currentTerm %v", vr.arg.CandidateID, vr.arg.Term, raft.currentTerm)
				vr.response <- pb.RequestVoteRet{Term: raft.currentTerm, VoteGranted: false}
			}
		case vr := <-voteResponseChan:
			// We received a response to a previous vote request.
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", vr.err)
			} else {
				log.Printf("Got response to vote request from %v", vr.peer)
				log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)

				if vr.ret.Term > raft.currentTerm {
					// reset as a follower
					log.Printf("Term %v greater than currect term %v, reset as a follower", vr.ret.Term, raft.currentTerm)
					raft.currentTerm = vr.ret.Term
					raft.votedFor = ""
					raft.voteCount = 0
					raft.state = 0
					restartTimer(timer, r, false)
				}

				if vr.arg.Term < raft.currentTerm {
					// this is an "out-dated" response, simply ignore it
					log.Printf("Received an out-dated vote response, ignored")
					break
				}

				if raft.state != 1 {
					log.Printf("Not a Candidate anymore, ignore it")
					break
				}

				if vr.ret.VoteGranted {
					raft.voteCount += 1
					if raft.voteCount >= raft.majorCount {
						// become a leader!
						log.Printf("Got enough votes, become a leader!")
						raft.state = 2
						raft.nextIndex = make(map[string]int64)
						raft.matchIndex = make(map[string]int64)
						raft.responseChans = make(map[int64]*chan pb.Result)

						// initialize nextIndex and matchIndex
						for _, p := range *peers {
							raft.nextIndex[p] = raft.lastLogIndex + 1
							raft.matchIndex[p] = -1
						}

						// now, send out the initial heartbeat message!
						log.Printf("Leader trigger a new round of heartbeat messages on term %v", raft.currentTerm)
						broadcastHeartbeat(&raft, &peerClients, &appendResponseChan, &installSnapshotResponseChan)

						// This would be a heartbeat timer
						restartTimer(timer, r, true)
					}
				}
			}
		case ar := <-appendResponseChan:
			if ar.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				log.Printf("Error calling RPC %v", ar.err)
				break
			}
			// We received a response to a previous AppendEntries RPC call
			log.Printf("Got append entries response from %v, result %v", ar.peer, ar.ret.Success)

			if ar.ret.Term > raft.currentTerm {
				// reset as a follower
				log.Printf("Term %v greater than currect term %v, reset as a follower", ar.ret.Term, raft.currentTerm)
				raft.currentTerm = ar.ret.Term
				raft.votedFor = ""
				raft.voteCount = 0
				raft.state = 0
				restartTimer(timer, r, false)
			}

			if raft.state != 2 {
				log.Printf("Not a leader anymore, ignore it")
				break
			}

			if raft.currentTerm > ar.arg.Term {
				log.Printf("Received an out-dated reponse, ignore it")
				break
			}

			if ar.ret.Success {
				// 1. update the information of this peer
				raft.matchIndex[ar.peer] = ar.arg.PrevLogIndex + int64(len(ar.arg.Entries))
				raft.nextIndex[ar.peer] = raft.matchIndex[ar.peer] + 1

				// 2. check if we could commit any logs
				for nextCommit := raft.commitIndex + 1; nextCommit <= raft.lastLogIndex; nextCommit++ {
					// check if we could commit "commitIndex + 1"
					count := 1 // we really have "replicated" the log to ourselves
					for p, _ := range peerClients {
						if raft.matchIndex[p] >= nextCommit {
							count += 1
						}
					}
					if count >= raft.majorCount {
						log.Printf("Commit log index: %v term: %v", nextCommit, raft.log[nextCommit-raft.firstLogIndex].Term)
						raft.commitIndex = nextCommit
					} else {
						break
					}
				}

				// 3. execute the commited logs and possibly send back to the clients
				for nextApply := raft.lastApplied + 1; nextApply <= raft.commitIndex; nextApply++ {
					if responseChan, ok := raft.responseChans[nextApply]; ok {
						log.Printf("Try to execute & response log entry index id: %v", nextApply)
						s.HandleCommand(InputChannelType{
							command:  *raft.log[nextApply-raft.firstLogIndex].Cmd,
							response: *responseChan,
						})
					} else {
						// reach here means that: the old leader send us this log and died...
						// then I magically become a leader, then I replicate this log succesfully and try to execute it
						// however, I don't know where to send it :P
						log.Printf("Try to execute log entry index id: %v", nextApply)
						c := make(chan pb.Result)
						arg := InputChannelType{
							command:  *raft.log[nextApply-raft.firstLogIndex].Cmd,
							response: c,
						}
						go func(parg *InputChannelType) {
							s.HandleCommand(*parg)
						}(&arg)
						<-c
					}
					raft.lastApplied = nextApply
				}

				// 4. if we have enough executed commands, we create a snapshot
				if raft.lastApplied-raft.firstLogIndex+1 > raft.maxCommitLog {
					createSnapShot(&raft, s)
				}

				// 5. send new appendEntry requests, if we have more logs
				if raft.lastLogIndex >= raft.nextIndex[ar.peer] {
					// Seems that we still need to check "-1" stuff..
					prevLogIndex := raft.nextIndex[ar.peer] - 1

					if prevLogIndex < raft.lastIncludedIndex {
						// This means we need to install a snapshot
						sendSnapShot(&raft, ar.peer, peerClients[ar.peer], &installSnapshotResponseChan)
					} else {
						var prevLogTerm int64 = -1
						if prevLogIndex == raft.lastIncludedIndex {
							prevLogTerm = raft.lastIncludedTerm
						} else {
							prevLogTerm = raft.log[prevLogIndex-raft.firstLogIndex].Term
						}

						log.Printf("Send peer %v the logs in range [%v, %v]", ar.peer, raft.nextIndex[ar.peer], raft.lastLogIndex)
						args := pb.AppendEntriesArgs{
							Term:         raft.currentTerm,
							LeaderID:     id,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,
							Entries:      raft.log[(raft.nextIndex[ar.peer] - raft.firstLogIndex):(raft.lastLogIndex - raft.firstLogIndex + 1)],
							LeaderCommit: raft.commitIndex,
						}
						go func(c pb.RaftClient,
							p string,
							args *pb.AppendEntriesArgs) {
							ret, err := c.AppendEntries(context.Background(), args)
							appendResponseChan <- AppendResponse{ret: ret, arg: args, err: err, peer: p}
						}(peerClients[ar.peer], ar.peer, &args)
					}
				}
			} else {
				// this means the peer's logs are lack behind, we need to reduce raft.nextIndex[] and retry
				// 1. update the information of this peer
				raft.nextIndex[ar.peer] = max(int64(0), raft.nextIndex[ar.peer]-5)

				// 2. retry!
				if raft.lastLogIndex >= raft.nextIndex[ar.peer] {
					prevLogIndex := raft.nextIndex[ar.peer] - 1
					if prevLogIndex < raft.lastIncludedIndex {
						// This means we need to install a snapshot
						sendSnapShot(&raft, ar.peer, peerClients[ar.peer], &installSnapshotResponseChan)
					} else {
						var prevLogTerm int64 = -1
						if prevLogIndex == raft.lastIncludedIndex {
							prevLogTerm = raft.lastIncludedTerm
						} else {
							prevLogTerm = raft.log[prevLogIndex-raft.firstLogIndex].Term
						}
						log.Printf("Send peer %v the logs in range [%v, %v]", ar.peer, raft.nextIndex[ar.peer], raft.lastLogIndex)
						args := pb.AppendEntriesArgs{
							Term:         raft.currentTerm,
							LeaderID:     id,
							PrevLogIndex: prevLogIndex,
							PrevLogTerm:  prevLogTerm,
							Entries:      raft.log[raft.nextIndex[ar.peer]-raft.firstLogIndex : (raft.lastLogIndex - raft.firstLogIndex + 1)],
							LeaderCommit: raft.commitIndex,
						}
						go func(c pb.RaftClient,
							p string,
							args *pb.AppendEntriesArgs) {
							ret, err := c.AppendEntries(context.Background(), args)
							appendResponseChan <- AppendResponse{ret: ret, arg: args, err: err, peer: p}
						}(peerClients[ar.peer], ar.peer, &args)
					}
				}
			}
		case is := <-raft.InstallSnapshotChan:
			log.Printf("Received install snapshot from %v, term %v", is.arg.LeaderID, is.arg.Term)

			if is.arg.Term < raft.currentTerm {
				log.Printf("Reject snapshot from %v, due to arg.Term %v smaller than term %v",
					is.arg.LeaderID, is.arg.Term, raft.currentTerm)
				is.response <- pb.InstallSnapshotRet{Term: raft.currentTerm}
				break
			}

			if is.arg.Term >= raft.currentTerm {
				// Transit to follower
				raft.state = 0
				raft.voteCount = 0
				raft.currentTerm = is.arg.Term
				raft.votedFor = is.arg.LeaderID
			}

			if is.arg.LastIncludedIndex <= raft.lastApplied {
				log.Printf("Ignore snapshot from %v, due to already executed", is.arg.LeaderID)
				is.response <- pb.InstallSnapshotRet{Term: raft.currentTerm}
				break
			}

			log.Printf("Reset server state using the snapshot")
			s.RestoreSnapshot(is.arg.Data)
			raft.log = make([]*pb.Entry, 0)
			raft.firstLogIndex = is.arg.LastIncludedIndex + 1
			raft.lastLogIndex = is.arg.LastIncludedIndex
			raft.lastLogTerm = is.arg.LastIncludedTerm
			raft.commitIndex = is.arg.LastIncludedIndex
			raft.lastApplied = is.arg.LastIncludedIndex
			raft.snapshotData = is.arg.Data
			raft.lastIncludedIndex = is.arg.LastIncludedIndex
			raft.lastIncludedTerm = is.arg.LastIncludedTerm

			is.response <- pb.InstallSnapshotRet{Term: raft.currentTerm}

			// restart timer
			restartTimer(timer, r, false)

		case sr := <-installSnapshotResponseChan:
			if sr.err != nil {
				log.Printf("Error calling RPC %v", sr.err)
				break
			}

			if sr.ret.Term > raft.currentTerm {
				// reset as a follower
				log.Printf("Term %v greater than currect term %v, reset as a follower", sr.ret.Term, raft.currentTerm)
				raft.currentTerm = sr.ret.Term
				raft.votedFor = ""
				raft.voteCount = 0
				raft.state = 0
				restartTimer(timer, r, false)
			}

			if sr.arg.Term < raft.currentTerm {
				log.Printf("Ignore an out-dated InstallSnapshot response")
				break
			}

			// update the information for this peer
			raft.matchIndex[sr.peer] = sr.arg.LastIncludedIndex
			raft.nextIndex[sr.peer] = raft.matchIndex[sr.peer] + 1
		}
	}
	log.Printf("Strange to arrive here")
}
