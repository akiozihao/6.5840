package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type ROLE uint8

const (
	FOLLOWER ROLE = iota
	CANDIDATE
	LEADER
)

const (
	HeartTime    = 100
	ElectionTime = 300
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	log         []Entry

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leader
	nextIndex  []int
	matchIndex []int

	// inner
	role          ROLE
	heartBeatTime time.Time
	electionTime  time.Time

	// apply
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		// be follower
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	// if voteFor is null or candidateId
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate's log is at least as up-to-date as receivers'log
		if rf.getLastLogTerm() < args.LastLogTerm ||
			(rf.getLastLogTerm() == args.LastLogTerm && rf.getLastLogIndex() <= args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			// if election timeout elapses without granting vote to candidate,convert to candidate
			// so need reset electionTime
			rf.resetElectionTimer()
			reply.VoteGranted = true
			Debug(dVote, "S%d vote to S%d", rf.me, args.CandidateId)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// if election timeout elapses  without receiving AppendEntries RPC from current leader
	// need reset electionTimer
	rf.resetElectionTimer()
	if rf.currentTerm < args.Term {
		// be follower
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	// reply false if log doesn't contain an entry at prevLogIndex whose
	// term matches prevLogTerm
	entry, exist := rf.getByIndex(args.PrevLogIndex)
	if !exist || entry.Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	reply.Success = true
	if len(args.Entries) > 0 {
		oldLogSize := len(rf.log)
		rf.mergeLog(args.PrevLogIndex+1, args.Entries)
		if len(rf.log) != oldLogSize {
			Debug(dClient, "S%d new log = %v", rf.me, rf.log)
		}
	}

	// if leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit,index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit >= rf.getLastLogIndex() {
			rf.commitIndex = rf.getLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		if rf.commitIndex != oldCommitIndex {
			Debug(dClient, "S%d commitIndex %d -> %d", rf.me, oldCommitIndex, rf.commitIndex)
		}
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}
	isLeader = true
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	term = rf.getLastLogTerm()
	index = rf.getLastLogIndex()
	Debug(dLeader, "S%d add log %v, index %d", rf.me, entry, index)
	return
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		timeElapse := time.Since(rf.electionTime)
		if timeElapse < time.Duration(ElectionTime+(rand.Int63()%300))*time.Millisecond || rf.role == LEADER {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// start election
		rf.role = CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetElectionTimer()

		// state
		term := rf.currentTerm
		lastLogIndex := rf.getLastLogIndex()
		lastLogTerm := rf.getLastLogTerm()
		voteCnt := 1
		rf.mu.Unlock()
		Debug(dVote, "S%d Term %d request vote ", rf.me, term)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			i := i
			// send RequestVote RPC to all other servers.
			go func() {
				args := &RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := &RequestVoteReply{}

				ok := rf.sendRequestVote(i, args, reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != term || rf.role != CANDIDATE {
					Debug(dCandidate, "S%d status change! now role=%d, term=%d", rf.me, rf.role, rf.currentTerm)
					return
				}

				if rf.currentTerm < reply.Term {
					// to follower
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					return
				}

				if reply.VoteGranted {
					voteCnt++
					if voteCnt > len(rf.peers)/2 && voteCnt-1 <= len(rf.peers)/2 {
						// be leader
						rf.role = LEADER
						rf.resetMatchIndex()
						rf.resetNextIndex()
						Debug(dVote, "S%d become leader", rf.me)
					}
				}

			}()
		}

	}
}

func (rf *Raft) heartBeat() {
	for !rf.killed() {
		rf.mu.Lock()
		timeElapse := time.Since(rf.heartBeatTime)
		if rf.role != LEADER || timeElapse < time.Duration(HeartTime)*time.Millisecond {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		// send heartBeat
		rf.resetHeartBeatTimer()
		term := rf.currentTerm
		rf.mu.Unlock()
		// Debug(dVote, "S%d Term %d send heart", rf.me, term)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			i := i
			go func() {
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Term:     term,
					LeaderId: rf.me,
				}
				reply := &AppendEntriesReply{}

				//  if last log index >= nextIndex for a follower: send
				// AppendEntries RPC with log entries starting at nextIndex
				if rf.getLastLogIndex() >= rf.nextIndex[i] {
					args.Entries = rf.getFromIndex(rf.nextIndex[i])
					Debug(dLeader, "S%d send log to S%d, log = %v", rf.me, i, args.Entries)
				}
				args.PrevLogIndex = rf.nextIndex[i] - 1
				entry, exist := rf.getByIndex(args.PrevLogIndex)
				if !exist {
					panic(fmt.Sprintf("S%d entry not exist, index = %d", rf.me, args.PrevLogIndex))
				}
				args.PrevLogTerm = entry.Term
				args.LeaderCommit = rf.commitIndex

				rf.mu.Unlock()

				ok := rf.sendAppendEntries(i, args, reply)

				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					return
				}

				if rf.role != LEADER || rf.currentTerm != term {
					Debug(dLeader, "S%d status change! now role=%d, term=%d", rf.me, rf.role, rf.currentTerm)
					return
				}

				if reply.Success {
					if len(args.Entries) > 0 {
						rf.nextIndex[i] += len(args.Entries)
						rf.matchIndex[i] = rf.nextIndex[i] - 1

						// if there exist an N such that N > commitIndex, a majority
						// of matchIndex[i] >= N, and log[N].term == currentTerm
						// set commitIndex = N
						for N := rf.matchIndex[i]; N > rf.commitIndex; N-- {
							cnt := 1
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								if rf.matchIndex[i] >= N {
									cnt++
									if cnt > len(rf.peers)/2 && cnt-1 <= len(rf.peers)/2 {
										Debug(dLeader, "S%d commitIndex %d -> %d", rf.me, rf.commitIndex, N)
										rf.commitIndex = N
										// apply
										if rf.commitIndex > rf.lastApplied {
											rf.applyCond.Signal()
										}
										break
									}
								}
							}
						}
					}
				} else {
					rf.nextIndex[i]--
					Debug(dTest, "S%d nextIndex[%d]--", rf.me, i)
				}

			}()
		}
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			Debug(dLog, "S%d start apply", rf.me)
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				entry, exist := rf.getByIndex(rf.lastApplied)
				if !exist {
					panic("applier not exist")
				}
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied,
				}

				rf.applyCh <- msg
				Debug(dLog, "S%d apply msg %v", rf.me, msg)
			}
		} else {
			rf.applyCond.Wait()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.log = []Entry{{Term: 0, Command: nil}}
	rf.heartBeatTime = time.Now()
	rf.electionTime = time.Now()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeat()
	go rf.applier()
	return rf
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTime = time.Now()
}

func (rf *Raft) resetHeartBeatTimer() {
	rf.heartBeatTime = time.Now()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getLastLogIndex()].Term
}

func (rf *Raft) getByIndex(idx int) (entry Entry, exist bool) {
	if idx > rf.getLastLogIndex() {
		return Entry{}, false
	}
	return rf.log[idx], true
}

// get Entries which index >= idx
func (rf *Raft) getFromIndex(idx int) (res []Entry) {
	// use res = rf.log[idx:]  data race 
	res = make([]Entry, 0)
	res = append(res, rf.log[idx:]...)
	return
}

// if an existing entry conflict with a new one
// (same index but different term), delete the existing entry and all that follow it
// Append any new entries not already in the log
func (rf *Raft) mergeLog(startIndex int, entries []Entry) {
	i, j := startIndex, 0
	for ; j < len(entries); i, j = i+1, j+1 {
		entry, exist := rf.getByIndex(i)
		if !exist {
			rf.log = append(rf.log, entries[j:]...)
			break
		}
		if entry.Term != entries[j].Term {
			// delete the existing entry and all that follow it
			rf.log = rf.log[:i]
			rf.log = append(rf.log, entries[j:]...)
			break
		}
	}
}

func (rf *Raft) resetNextIndex() {
	n := rf.getLastLogIndex() + 1
	for i := 0; i < len(rf.nextIndex); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = n
	}
	Debug(dLeader, "S%d nextIndex = %v", rf.me, rf.nextIndex)
}

func (rf *Raft) resetMatchIndex() {
	for i := 0; i < len(rf.nextIndex); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = 0
	}
}
