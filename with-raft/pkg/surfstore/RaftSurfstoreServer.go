package surfstore

import (
	context "context"
	"log"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	isLeader bool
	term     int64
	log      []*UpdateOperation

	metaStore *MetaStore

	commitIndex int64
	lastApplied int64

	pendingHeartBeats  []chan bool
	pendingCheckStates []chan bool

	// Server Info
	ip         string
	ipList     []string
	serverId   int64
	nextIndex  []int64
	matchIndex []int64

	// Leader protection
	isLeaderMutex *sync.RWMutex

	// worker protection
	workerMutex *sync.RWMutex

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// TODO maybe change this logic like updatefile to block?
	for {
		success, err := s.checkMajorityStates()
		if err != nil {
			log.Println(err)
			return nil, err
		}
		if success.Flag {
			break
		}
	}
	return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	// TODO maybe change this logic like updatefile to block?
	for {
		success, err := s.checkMajorityStates()
		if err != nil {
			return nil, err
		}
		if success.Flag {
			break
		}
	}
	return s.metaStore.GetBlockStoreAddr(ctx, &emptypb.Empty{})
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.isLeaderMutex.RLock()
	curIsLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !curIsLeader {
		return nil, ERR_NOT_LEADER
	}

	s.isCrashedMutex.RLock()
	curIsCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if curIsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.workerMutex.Lock()
	curTerm := s.term
	op := UpdateOperation{
		Term:         curTerm,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &op)
	logIdx := len(s.log) - 1
	s.workerMutex.Unlock()

	commited := make(chan bool)

	// used for check if the logIdx is committed
	go s.commitWorker(logIdx, commited)

	// parallel send appendEntries
	for idx := range s.ipList {
		if idx == int(s.serverId) {
			continue
		}

		s.workerMutex.RLock()
		curNextIndex := s.nextIndex[idx]
		s.workerMutex.RUnlock()

		if int64(logIdx) >= curNextIndex {
			go s.commitEntry(int64(idx))
		}
	}

	// TODO: in this progress, leader may become follower. in this case, success will block forever. but the client will timeout to find next available leader
	success := <-commited
	if success {
		s.workerMutex.Lock()
		s.commitIndex = int64(logIdx)
		s.lastApplied = int64(logIdx)
		s.workerMutex.Unlock()
		// s.SendHeartbeat(context.Background(), &emptypb.Empty{})
		return s.metaStore.UpdateFile(ctx, filemeta)
	} else {
		s.isLeaderMutex.RLock()
		curIsLeader := s.isLeader
		s.isLeaderMutex.RUnlock()

		if !curIsLeader {
			return nil, ERR_NOT_LEADER
		}

		s.isCrashedMutex.RLock()
		curIsCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()

		if curIsCrashed {
			return nil, ERR_SERVER_CRASHED
		}
	}
	return nil, nil
}

// this is the worker for update the commitIndex
func (s *RaftSurfstore) commitWorker(logIdx int, commited chan<- bool) {
	for {
		// this is only for the uncrashed leader
		s.isCrashedMutex.RLock()
		curIsCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()
		if curIsCrashed {
			commited <- false
			break
			// panic("crashed")
		}

		s.isLeaderMutex.RLock()
		curIsLeader := s.isLeader
		s.isLeaderMutex.RUnlock()
		if !curIsLeader {
			commited <- false
			break
			// panic("not leader")
		}

		matchCnt := 1

		for idx := range s.ipList {
			if idx == int(s.serverId) {
				continue
			}

			// protect matchIndex
			s.workerMutex.RLock()
			if s.matchIndex[idx] >= int64(logIdx) {
				matchCnt++
			}
			s.workerMutex.RUnlock()
		}

		if matchCnt > len(s.ipList)/2 {
			commited <- true
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx int64) {
	for {
		// this is only for the uncrashed leader
		s.isCrashedMutex.RLock()
		curIsCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()

		if curIsCrashed {
			break
			// panic("crashed")
		}

		s.isLeaderMutex.RLock()
		curIsLeader := s.isLeader
		s.isLeaderMutex.RUnlock()

		if !curIsLeader {
			break
			// panic("not leader")
		}

		// TODO maybe use consistent call here?
		conn, err := grpc.Dial(s.ipList[serverIdx], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Println(err)
		}
		client := NewRaftSurfstoreClient(conn)

		// protect nextIndex
		s.workerMutex.RLock()
		curNextIndex := s.nextIndex[serverIdx]

		prevLogTerm := int64(0)
		if curNextIndex != 0 {
			prevLogTerm = s.log[curNextIndex-1].Term
		}

		// protect commitIndex
		curCommitIndex := s.commitIndex

		curLog := make([]*UpdateOperation, len(s.log))
		copy(curLog, s.log)

		curTerm := s.term

		s.workerMutex.RUnlock()

		input := &AppendEntryInput{Term: curTerm, PrevLogIndex: curNextIndex - 1, PrevLogTerm: prevLogTerm, Entries: curLog[curNextIndex:], LeaderCommit: curCommitIndex}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		output, err := client.AppendEntries(ctx, input)

		// if crashed, retry appendEntries
		if err != nil {
			conn.Close()
			continue
		}

		if output.Success {
			//If successful: update nextIndex and matchIndex for follower
			s.workerMutex.Lock()
			// protect nextIndex and matchIndex
			s.nextIndex[serverIdx] = output.MatchedIndex + 1

			s.matchIndex[serverIdx] = output.MatchedIndex
			s.workerMutex.Unlock()
			conn.Close()
			return
		} else {
			// for case 2 in false of appendentries
			s.workerMutex.Lock()
			if output.Term <= curTerm {
				s.nextIndex[serverIdx] = s.nextIndex[serverIdx] - 1
			} else {
				// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
				s.term = output.Term
				s.isLeader = false
			}
			s.workerMutex.Unlock()
			conn.Close()
		}
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// Should return an “isCrashed” error; procedure has no effect if server is crashed
	s.isCrashedMutex.RLock()
	curIsCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if curIsCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.workerMutex.Lock()
	curTerm := s.term

	//1. Reply false if term < currentTerm (§5.1)
	if input.Term < curTerm {
		// TODO : which value should assign to MatchedIndex?
		output := &AppendEntryOutput{ServerId: s.serverId, Term: curTerm, Success: false, MatchedIndex: 0}
		s.workerMutex.Unlock()
		return output, nil
	} else if input.Term > curTerm {
		// if one server’s current term is smaller than the other’s, then it updates its current term to the larger value
		// if it is the leader, it become follower
		s.term = input.Term
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()
	}

	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
	//matches prevLogTerm (§5.3)
	// input.PrevLogIndex>=len(curLog)
	if input.PrevLogIndex >= int64(len(s.log)) {
		output := &AppendEntryOutput{ServerId: s.serverId, Term: curTerm, Success: false, MatchedIndex: 0}
		s.workerMutex.Unlock()
		return output, nil
	}

	//3. If an existing entry conflicts with a new one (same index but different
	//terms), delete the existing entry and all that follow it (§5.3)
	// input.PrevLogIndex<len(curLog) but log[prevLogIndex].Term != prevLogTerm
	// have no prevlog
	if input.PrevLogIndex == -1 {
		s.log = make([]*UpdateOperation, 0)
	} else {
		if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			// delete
			s.log = s.log[:input.PrevLogIndex]
			output := &AppendEntryOutput{ServerId: s.serverId, Term: curTerm, Success: false, MatchedIndex: 0}
			s.workerMutex.Unlock()
			return output, nil
		}
		s.log = s.log[:input.PrevLogIndex+1]
	}

	// if we pass the above cases, we can do the append
	// 4. Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	curCommitIndex := s.commitIndex
	if input.LeaderCommit > curCommitIndex {
		minIndex := input.LeaderCommit
		if input.LeaderCommit >= int64(len(s.log)) {
			minIndex = int64(len(s.log) - 1)
		}
		s.commitIndex = minIndex
	}

	// check the lastApplied
	for {
		if s.commitIndex == s.lastApplied {
			break
		}
		if curCommitIndex < s.lastApplied {
			log.Println("find commitIndex < lastApplied!")
			break
		}

		op := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(context.Background(), op.FileMetaData)
		s.lastApplied = s.lastApplied + 1
	}

	output := &AppendEntryOutput{ServerId: s.serverId, Term: curTerm, Success: true, MatchedIndex: int64(len(s.log) - 1)}
	s.workerMutex.Unlock()
	return output, nil
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// todo: other potential var should be set
	s.isCrashedMutex.RLock()
	curIsCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if curIsCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()

	s.workerMutex.Lock()
	s.term += 1

	lastLogIndex := int64(len(s.log)) - 1

	// reinitialize nextIndex and matchIndex on the leader
	for idx := range s.ipList {
		s.nextIndex[idx] = lastLogIndex + 1
		s.matchIndex[idx] = -1
	}
	s.workerMutex.Unlock()
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isLeaderMutex.RLock()
	curIsLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !curIsLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	s.isCrashedMutex.RLock()
	curIsCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if curIsCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	heartBeatCh := make(chan int, len(s.ipList)-1)
	// normal operation
	for idx := range s.ipList {
		if idx == int(s.serverId) {
			continue
		}
		go s.heartBeat(int64(idx), heartBeatCh)
	}

	heartBeatCnt := 0
	for {
		if heartBeatCnt == len(s.ipList)-1 {
			break
		}
		temp := <-heartBeatCh
		heartBeatCnt += temp
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) heartBeat(serverIdx int64, heartBeatCh chan<- int) {
	for {
		// this is only for the uncrashed leader
		// s.isCrashedMutex.RLock()
		// curIsCrashed := s.isCrashed
		// s.isCrashedMutex.RUnlock()

		// if curIsCrashed {
		// 	heartBeatCh <- 1
		// 	return
		// }

		// s.isLeaderMutex.RLock()
		// curIsLeader := s.isLeader
		// s.isLeaderMutex.RUnlock()

		// if !curIsLeader {
		// 	heartBeatCh <- 1
		// 	return
		// }

		conn, err := grpc.Dial(s.ipList[serverIdx], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Println(err)
		}
		client := NewRaftSurfstoreClient(conn)

		// protect nextIndex
		s.workerMutex.RLock()
		curNextIndex := s.nextIndex[serverIdx]

		// protect commitIndex
		curCommitIndex := s.commitIndex

		prevLogTerm := int64(0)
		if curNextIndex != 0 {
			prevLogTerm = s.log[curNextIndex-1].Term
		}

		curTerm := s.term
		curLog := make([]*UpdateOperation, len(s.log))
		copy(curLog, s.log)
		emptyEntries := make([]*UpdateOperation, 0)
		if len(curLog)-1 >= int(curNextIndex) {
			emptyEntries = curLog[curNextIndex:]
		}
		s.workerMutex.RUnlock()

		input := &AppendEntryInput{Term: curTerm, PrevLogIndex: curNextIndex - 1, PrevLogTerm: prevLogTerm, Entries: emptyEntries, LeaderCommit: curCommitIndex}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		output, err := client.AppendEntries(ctx, input)
		if err != nil {
			heartBeatCh <- 1
			conn.Close()
			return
		} else {
			if output.Success {
				//If successful: update nextIndex and matchIndex for follower
				s.workerMutex.Lock()
				// protect nextIndex and matchIndex
				s.nextIndex[serverIdx] = output.MatchedIndex + 1
				s.matchIndex[serverIdx] = output.MatchedIndex
				s.workerMutex.Unlock()
				heartBeatCh <- 1
				conn.Close()
				return
			} else {
				if output.Term > curTerm {
					// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
					s.workerMutex.Lock()
					s.term = output.Term
					s.isLeader = false
					s.workerMutex.Unlock()
					heartBeatCh <- 1
					conn.Close()
					return
				} else {
					s.nextIndex[serverIdx] = s.nextIndex[serverIdx] - 1
					conn.Close()
				}
			}
		}
	}
}

func (s *RaftSurfstore) checkMajorityStates() (*Success, error) {
	s.isLeaderMutex.RLock()
	curIsLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !curIsLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	s.isCrashedMutex.RLock()
	curIsCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if curIsCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	unCrashedCnt := 0
	s.pendingCheckStates = make([]chan bool, 0)
	// normal operation
	for idx := range s.ipList {
		state := make(chan bool)
		s.pendingCheckStates = append(s.pendingCheckStates, state)
		go s.checkState(int64(idx))
	}

	for idx := range s.ipList {
		temp := <-s.pendingCheckStates[idx]
		if temp {
			unCrashedCnt += 1
		}
	}

	if unCrashedCnt > len(s.ipList)/2 {
		return &Success{Flag: true}, nil
	}

	return &Success{Flag: false}, nil
}

func (s *RaftSurfstore) checkState(serverIdx int64) {
	if serverIdx == s.serverId {
		s.pendingCheckStates[serverIdx] <- true
		return
	}
	conn, err := grpc.Dial(s.ipList[serverIdx], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Println(err)
	}
	client := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	output, _ := client.IsCrashed(ctx, &emptypb.Empty{})
	if output.IsCrashed {
		s.pendingCheckStates[serverIdx] <- false
	} else {
		s.pendingCheckStates[serverIdx] <- true
	}
	conn.Close()
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	s.isCrashedMutex.RLock()
	curIsCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	return &CrashedState{IsCrashed: curIsCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
