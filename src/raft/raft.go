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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	log         Log

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

	// for snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
	snapShot          []byte
	installMsg        ApplyMsg
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.persister.ReadSnapshot())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log Log
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic(fmt.Sprintf("S%d readPersist error!", rf.me))
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = lastIncludedIndex
		rf.commitIndex = lastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log.entry(index).Term
	rf.log.cutStart(index - rf.log.Index0)
	rf.snapShot = snapshot
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
	if len(rf.log.Log) > 0 {
		Debug(dSnap, "S%d snapshots to %d log[%d - %d]", rf.me, index, rf.log.Index0, rf.log.lastIndex())
	}
}

func (rf *Raft) SnapShotPersist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapShot)
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

	// 2C optimized
	FirstIndex int
}

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapShotReply struct {
	Term int
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	rf.snapShot = args.Data
	// if existing log entry has same index and term as snapshot's
	// last include entry,retain log entries following it and reply
	if rf.log.lastIndex() >= args.LastIncludedIndex && rf.log.entry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.log.cutStart(args.LastIncludedIndex - rf.log.Index0)
	} else {
		// discard the entire log
		rf.log = mkLog([]Entry{{args.LastIncludedTerm, nil}}, args.LastIncludedIndex)
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	applyMsh := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapShot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.installMsg = applyMsh
	rf.applyCond.Signal()
	Debug(dSnap, "S%d applyMsg.SnapshotIndex = %d, SnapshotTerm = %d", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
	rf.SnapShotPersist()
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
		if rf.log.lastEntry().Term < args.LastLogTerm ||
			(rf.log.lastEntry().Term == args.LastLogTerm && rf.log.lastIndex() <= args.LastLogIndex) {
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
		rf.persist()
	}
	reply.Term = rf.currentTerm
	// reply false if log doesn't contain an entry at prevLogIndex whose
	// term matches prevLogTerm
	if rf.log.lastIndex() < args.PrevLogIndex {
		reply.FirstIndex = -1
		return
	}
	if rf.log.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// entry.Term != args.PrevLogTerm
		conflictTerm := rf.log.entry(args.PrevLogIndex).Term
		j := args.PrevLogIndex
		for j > rf.log.start() {
			t := rf.log.entry(j)
			if t.Term != conflictTerm {
				break
			}
			j--
		}
		reply.FirstIndex = j + 1
		return
	}

	reply.Success = true

	if len(args.Entries) > 0 {
		rf.mergeLog(args.PrevLogIndex+1, args.Entries)
		Debug(dClient, "S%d new log = %v", rf.me, rf.log)
	}

	// if leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit,index of last new entry)

	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		if args.LeaderCommit >= rf.log.lastIndex() {
			rf.commitIndex = rf.log.lastIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		if rf.commitIndex != oldCommitIndex {
			Debug(dLog2, "S%d commitIndex %d -> %d, log = %v", rf.me, oldCommitIndex, rf.commitIndex, rf.log)
		}

		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	}
	rf.persist()
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

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
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
	rf.log.append(entry)
	rf.persist()
	term = rf.currentTerm
	index = rf.log.lastIndex()
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
	Debug(dTest, "S%d killed, log = %v", rf.me, rf.log)
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
		rf.persist()
		// state
		term := rf.currentTerm
		lastLogIndex := rf.log.lastIndex()
		lastLogTerm := rf.log.lastEntry().Term
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
					rf.persist()
					return
				}

				if reply.VoteGranted {
					voteCnt++
					if voteCnt > len(rf.peers)/2 && voteCnt-1 <= len(rf.peers)/2 {
						// be leader
						rf.role = LEADER
						rf.resetMatchIndex()
						rf.resetNextIndex()
						rf.persist()
						Debug(dVote, "S%d become leader, log = %v", rf.me, rf.log)
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
					Term:         term,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				if rf.nextIndex[i] <= rf.log.start() {
					term := rf.currentTerm
					lastIncludeIndex := rf.lastIncludedIndex
					lastIncludeTerm := rf.lastIncludedTerm
					args := &InstallSnapShotArgs{
						Term:              term,
						LeaderId:          rf.me,
						LastIncludedIndex: lastIncludeIndex,
						LastIncludedTerm:  lastIncludeTerm,
						Data:              rf.snapShot,
					}
					reply := &InstallSnapShotReply{}
					rf.mu.Unlock()
					Debug(dSnap, "S%d -> S%d, args.LastIncludedIndex = %d, args.lastIncludedTerm = %d,log = %v", rf.me, i, args.LastIncludedIndex, args.LastIncludedTerm, rf.log)
					ok := rf.sendInstallSnapShot(i, args, reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = FOLLOWER
						rf.votedFor = -1
						rf.persist()
						return
					}
					if rf.currentTerm != term {
						return
					}
					if args.LastIncludedIndex+1 > rf.nextIndex[i] {
						rf.nextIndex[i] = args.LastIncludedIndex + 1
					}
					rf.matchIndex[i] = rf.nextIndex[i] - 1
					Debug(dSnap, "S%d -> S%d, nextIndex[i] = %d, matchIndex[i] = %d", rf.me, i, rf.nextIndex[i], rf.matchIndex[i])

					return
				}
				//  if last log index >= nextIndex for a follower: send
				// AppendEntries RPC with log entries starting at nextIndex
				if rf.log.lastIndex() >= rf.nextIndex[i] {
					t := rf.log.slice(rf.nextIndex[i])
					args.Entries = make([]Entry, len(t))
					copy(args.Entries, t)

					args.PrevLogIndex = rf.nextIndex[i] - 1
					args.PrevLogTerm = rf.log.entry(args.PrevLogIndex).Term
					Debug(dLeader, "S%d send log to S%d, nextIndex[i] = %d,args = %v, rf.log = %v", rf.me, i, rf.nextIndex[i], args, rf.log)

				} else {
					args.PrevLogIndex = rf.log.lastIndex()
					args.PrevLogTerm = rf.log.lastEntry().Term
				}

				rf.mu.Unlock()

				ok := rf.sendAppendEntries(i, args, reply)

				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term < rf.currentTerm {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					return
				}

				if rf.role != LEADER || rf.currentTerm != term {
					Debug(dLeader, "S%d status change! now role=%d, term=%d", rf.me, rf.role, rf.currentTerm)
					return
				}

				if reply.Success {
					if len(args.Entries) > 0 {
						// rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
						// rf.matchIndex[i] = rf.nextIndex[i] - 1

						newNext := args.PrevLogIndex + len(args.Entries) + 1
						newMatch := args.PrevLogIndex + len(args.Entries)
						if newNext > rf.nextIndex[i] {
							rf.nextIndex[i] = newNext
						}
						if newMatch > rf.matchIndex[i] {
							rf.matchIndex[i] = newMatch
						}
						// if there exist an N such that N > commitIndex, a majority
						// of matchIndex[i] >= N, and log[N].term == currentTerm
						// set commitIndex = N
						for N := rf.matchIndex[i]; N > rf.commitIndex; N-- {
							cnt := 1
							for i := 0; i < len(rf.peers); i++ {
								if i == rf.me {
									continue
								}
								entry := rf.log.entry(N)
								// fix figure-8 5.4.2
								if entry.Term != rf.currentTerm {
									break
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
					// rf.nextIndex[i]--
					old := rf.nextIndex[i]

					if reply.FirstIndex == -1 {
						j := args.PrevLogIndex
						for j >= rf.log.start() {
							t := rf.log.entry(j)
							if t.Term != args.PrevLogTerm {
								break
							}
							j--
						}
						rf.nextIndex[i] = int(math.Min(float64(rf.nextIndex[i]), float64(j+1)))
					} else {
						rf.nextIndex[i] = int(math.Min(float64(reply.FirstIndex), float64(rf.nextIndex[i])))
					}
					Debug(dLog2, "S%d rf.nextIndex[%d] %d -> %d", rf.me, i, old, rf.nextIndex[i])
					// Debug(dTest, "S%d nextIndex[%d]--", rf.me, i)
				}
				rf.persist()
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
				entry := rf.log.entry(rf.lastApplied)
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
				Debug(dLog, "S%d apply msg %v", rf.me, msg)
			}
		} else if len(rf.installMsg.Snapshot) > 0 {
			rf.mu.Unlock()
			rf.applyCh <- rf.installMsg
			rf.mu.Lock()
			rf.installMsg = ApplyMsg{}
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
	rf.log = mkLogEmpty()
	rf.heartBeatTime = time.Now()
	rf.electionTime = time.Now()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapShot = persister.ReadSnapshot()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapShot = persister.ReadSnapshot()
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

func (rf *Raft) resetNextIndex() {
	n := rf.log.lastIndex() + 1
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

// if an existing entry conflict with a new one
// (same index but different term), delete the existing entry and all that follow it
// Append any new entries not already in the log
func (rf *Raft) mergeLog(startIndex int, entries []Entry) {
	i, j := startIndex, 0
	for ; j < len(entries); i, j = i+1, j+1 {
		if i <= rf.log.lastIndex() {
			if rf.log.entry(i).Term == entries[j].Term {
				continue
			}
			rf.log.cutEnd(i)
			rf.log.append(entries[j])
		} else {
			rf.log.append(entries[j])
		}
	}
}
