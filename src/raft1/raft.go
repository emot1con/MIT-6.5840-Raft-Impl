package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type NodeRole string

const (
	RoleLeader    NodeRole = "leader"
	RoleFollower  NodeRole = "follower"
	RoleCandidate NodeRole = "candidate"
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Role      NodeRole
	Log       []LogEntry
	Term      int
	VotedFor  int
	LastHeard time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).

	term = rf.Term
	isleader = rf.Role == RoleLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	ID           int
	Term         int
	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	IsVoted bool
	Term    int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.Term {
		reply.IsVoted = false
		reply.Term = rf.Term
		return
	}

	if args.Term > rf.Term {
		rf.Term = args.Term
		rf.VotedFor = -1
		rf.Role = RoleFollower
	}

	lastIndex := len(rf.Log) - 1
	lastTerm := rf.Log[len(rf.Log)-1]

	isVoteCandidateValid := rf.VotedFor == -1 || rf.VotedFor == args.ID
	logCandidateIsNewer := (args.LastLogTerm == lastTerm.Term && args.LastLogIndex >= lastIndex) ||
		args.LastLogTerm > lastTerm.Term

	if isVoteCandidateValid && logCandidateIsNewer {
		rf.VotedFor = args.ID
		rf.LastHeard = time.Now()
		reply.IsVoted = true
	} else {
		reply.IsVoted = false
	}
	log.Printf("[%d] RV dari %d (term %d) -> granted=%v, termku=%d",
		rf.me, args.ID, args.Term, reply.IsVoted, rf.Term)
	reply.Term = rf.Term
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		timeout := time.Duration(500+rand.Int63()%500) * time.Millisecond
		startElection := rf.Role != RoleLeader && time.Since(rf.LastHeard) > time.Duration(timeout)
		rf.mu.Unlock()

		if startElection {
			rf.startElection()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.Role = RoleFollower
	rf.Log = []LogEntry{{
		Command: nil,
		Term:    0,
	}}
	rf.Term = 0
	rf.VotedFor = -1
	rf.LastHeard = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.Role = RoleCandidate
	rf.Term++
	rf.VotedFor = rf.me
	rf.LastHeard = time.Now()

	args := new(RequestVoteArgs)

	args.ID = rf.me
	args.Term = rf.Term
	args.LastLogIndex = len(rf.Log) - 1
	args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
	currTerm := rf.Term
	votes := 1

	rf.mu.Unlock()
	log.Printf("[%d] mulai election, term=%d", rf.me, currTerm)

	for idx := range rf.peers {
		if idx == rf.me {
			continue

		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if ok := rf.sendRequestVote(peer, args, reply); !ok {
				log.Printf("[%d] RPC ke %d GAGAL", rf.me, peer)

				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.Term {
				rf.Term = reply.Term
				rf.VotedFor = -1
				rf.Role = RoleFollower
				return
			}

			if rf.Role != RoleCandidate || rf.Term != currTerm {
				return
			}

			if reply.IsVoted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.Role = RoleLeader
					log.Printf("[%d] JADI LEADER, term=%d", rf.me, rf.Term)

					go rf.startHeartBeatLoop(rf.Term)
				}
			}
		}(idx)
	}
}

type AppendEntriesArgs struct {
	ID   int
	Term int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.Term {
		reply.Term = rf.Term
		reply.Success = false
		return
	}

	if args.Term > rf.Term {
		rf.Term = args.Term
		rf.VotedFor = -1
	}

	rf.Role = RoleFollower
	rf.LastHeard = time.Now()

	reply.Term = rf.Term
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startHeartBeatLoop(leaderTerm int) {
	for {
		rf.mu.Lock()

		if rf.Role != RoleLeader || rf.Term != leaderTerm {
			rf.mu.Unlock()
			return
		}

		args := new(AppendEntriesArgs)
		args.ID = rf.me
		args.Term = rf.Term

		rf.mu.Unlock()

		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}

			go func(peerIdx int) {
				reply := new(AppendEntriesReply)
				if ok := rf.sendAppendEntries(peerIdx, args, reply); !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.Term {
					rf.Role = RoleFollower
					rf.Term = reply.Term
					rf.VotedFor = -1
				}
			}(idx)
		}

		time.Sleep(120 * time.Millisecond)
	}
}

