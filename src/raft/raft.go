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
	//	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	FOLLOWER = 0
	CANDIDATE = 1
	LEADER = 2

	HEARBEAT_INTERVAL = 100
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type LogEntry struct {
	Id int64
	Command string
	Term int64
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	id int64
	role int //0:follower, 1:candidate, 2:leader


	isFollower chan bool

	// persistant
	currentTerm int64
	votedFor int64
	log []LogEntry

	// volatile
	commitIndex int64
	lastApplied int64

	// for candidates
	votes int64

	// volatile for leaders
	// todo: the type ?
	nextIndex int
	matchIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	isleader = rf.role==LEADER

	//fmt.Println(fmt.Sprintf("State. id=%v, term=%v, role=%v",rf.me, term, rf.role))
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int64
	CandidateId int64
	LastLogIndex int64
	LastLogTerm int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Msg string
	Term int64
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Msg = "ok"
	if args == nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("   server - requestVote", args.CandidateId, rf.votedFor, args.Term, rf.currentTerm)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if rf.currentTerm < args.Term {
		rf.beFollower(args.Term)
	}

	var lIndex int64 = -1
	var lTerm int64 = -1
	if len(rf.log)>0 {
		lastLog := rf.log[len(rf.log)-1]
		lIndex = lastLog.Id
		lTerm = lastLog.Term
	}

	if (rf.votedFor == 0 || rf.votedFor == args.CandidateId) &&
		(args.LastLogIndex > lIndex || args.LastLogIndex == lIndex && args.LastLogTerm >= lTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		// ?
		rf.beFollower(args.Term)
		rf.votedFor = args.CandidateId
	}
}

type AppendEntriesArgs struct {
	Term int64
	LeaderId int64
	PrevLogIndex int64
	PrevLogTerm int64
	Entries []LogEntry
	LeaderCommidIdx int64
}

type AppendEntriesReply struct {
	Term int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args == nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.beFollower(args.Term)

	reply.Success = true
	reply.Term = rf.currentTerm
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) appendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) reqVotes() {

}

func (rf *Raft) onTransToCandidate() {
	rf.mu.Lock()
	rf.currentTerm ++
	rf.role = CANDIDATE

	// vote for self
	rf.votes = 1
	rf.votedFor = rf.id
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.id,
		LastLogIndex: -1,
		LastLogTerm: -1,
	}
	rf.mu.Unlock()

	//start := time.Now().UnixNano()
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		i := idx
		go func() {
			rply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, rply)
			rf.mu.Lock()
			if ok {
				if rply.Term > rf.currentTerm {
					rf.beFollower(rply.Term)
				} else if rf.role != CANDIDATE || args.Term != rf.currentTerm {
					//fmt.Println("stale state")
				} else {
					if rply.VoteGranted {
						rf.votes ++
						if winMajority(rf.votes, int64(len(rf.peers))) {
							rf.role = LEADER
							// 此处不是为了reset timer，而是为了通知role变化
							// 如果不加，那么由于goroutine这里的role set比进入下一循环慢，
							//		so rf 仍以candidate身份进入下一循环select，等待下一轮onElection，然后就直接覆盖掉leader身份了
							rf.follower()
						}
					}
				}
			}
			rf.mu.Unlock()
		}()
	}
}

func (rf *Raft) onTransToLeader() {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.id,
	}
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}

		i := idx
		go func() {
			rply := &AppendEntriesReply{}
			ok := rf.appendEntries(i, args, rply)
			rf.mu.Lock()
			if ok {
				if rply.Term > rf.currentTerm {
					rf.beFollower(rply.Term)
				}
			}
			rf.mu.Unlock()
		}()
	}
}

func (rf *Raft) follower() {
	select {
	case <-rf.isFollower:
		doNothing()
	default:
		doNothing()
	}
	rf.isFollower <- true
}


func (rf *Raft) beFollower(term int64) {
	rf.votedFor = 0
	rf.votes = 0
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.follower()
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		randT := 600+rand.Int63n(250)
		switch role {
			case LEADER:
				//fmt.Println(" ==leader send hb", rf.id, time.Now().UnixNano()/1000000)
				rf.onTransToLeader()
				time.Sleep(HEARBEAT_INTERVAL * time.Millisecond)
			case FOLLOWER, CANDIDATE:
				select {
				case <-rf.isFollower:
				case <- time.After(time.Duration(randT) * time.Millisecond):
					rf.onTransToCandidate()
				}
		}
	}
}

func doNothing() {

}

func winMajority(votes, total int64) bool {
	return total%2==1 && votes > total/2 || total%2==0 && votes >= total/2
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.id = rand.Int63n(100000000)

	// Your initialization code here (2A, 2B, 2C).
	rf.isFollower = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
