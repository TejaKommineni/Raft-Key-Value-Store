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
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"path"
	"runtime"
	"sync"
	"time"
)

const LALL = false
const LRPC = LALL
const LRV = LALL
const LAE = LALL
const LLOG = LALL
const LIFACE = LALL
const LLOCK = LALL

const slowdown = time.Duration(2.0)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	nMajority int

	isLeader bool

	CurrentTerm int
	VotedFor    int
	LogTerms    []int
	Log         []interface{}

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	replicationProgress bool

	lastHeartbeat time.Time

	applyCh chan ApplyMsg

	killed bool

	LeaderId int

	lockTime time.Time
}

func (rf *Raft) debug(category bool, f string, args ...interface{}) {
	if !category {
		return
	}
	p := fmt.Sprintf("[%v] ", rf.me)
	s := fmt.Sprintf(f, args...)
	DPrintf(p + s)
}

func (rf *Raft) lock() {
	rf.mu.Lock()
	rf.lockTime = time.Now()
}

func (rf *Raft) unlock() {
	_, file, line, _ := runtime.Caller(1)
	lockDuration := time.Since(rf.lockTime)
	rf.mu.Unlock()
	if lockDuration > 10*time.Millisecond {
		rf.debug(LLOCK, "**** Long Raft Lockout time %v **** on unlock at %v:%v",
			lockDuration, path.Base(file), line)
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock()
	defer rf.unlock()
	return rf.CurrentTerm, rf.isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf)
	//rf.debug(LIFACE, "Saving Raft to persistent state")
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf)
	rf.debug(LIFACE, "After restore CurrentTerm %v LogLength %v", rf.CurrentTerm,
		len(rf.Log))
}

func (rf *Raft) dumpLog() {
	s := ""
	for i := 0; i < len(rf.Log); i++ {
		s += fmt.Sprintf("[%v %v] ", rf.LogTerms[i], rf.Log[i])
	}
	rf.debug(LLOG, "CI %v LA %v %v", rf.commitIndex, rf.lastApplied, s)
}

func (rf *Raft) doApplications() {
	rf.lock()
	for {
		if rf.killed {
			rf.unlock()
			return
		}

		stuffToDo := (rf.lastApplied < rf.commitIndex) &&
			(len(rf.Log) > rf.lastApplied+1)
		if !stuffToDo {
			rf.unlock()
			time.Sleep(1 * time.Millisecond * slowdown)
			rf.lock()
			continue
		}

		i := rf.lastApplied + 1
		rf.dumpLog()
		rf.debug(LIFACE, "<- APPLY %v %v", i, rf.Log[i])
		msg := ApplyMsg{i, rf.Log[i], false, []byte{}}
		rf.lastApplied = i

		if rf.Log[i] == nil {
			rf.debug(LIFACE, "[[ SKIPPING APPLY SINCE NIL ENTRY ]]")
		} else {
			rf.unlock()
			rf.applyCh <- msg
			rf.lock()
		}
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.lock()
	defer rf.unlock()

	rf.debug(LRPC, "-> RequestVote %v", args)

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		rf.debug(LRV, "Denied vote for %v on account of stale term", args.CandidateId)
		rf.debug(LRPC, "<- RequestVote Denied")
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.stepDown(args.Term)
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		myLogIndex := len(rf.Log) - 1
		myLogTerm := rf.LogTerms[len(rf.Log)-1]
		if args.LastLogTerm > myLogTerm ||
			(args.LastLogTerm == myLogTerm && args.LastLogIndex >= myLogIndex) {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.persist()
			rf.lastHeartbeat = time.Now()
			rf.debug(LRPC, "<- RequestVote VotedFor %v", args.CandidateId)
			return
		} else {
			rf.debug(LRV,
				"Denied vote for %v due to log completeness: me (%v, %v), them (%v, %v)",
				args.CandidateId,
				myLogTerm,
				myLogIndex,
				args.LastLogTerm,
				args.LastLogIndex)
			rf.dumpLog()
		}
	} else {
		rf.debug(LRV, "Denied vote for %v on account of already voted for %v",
			args.CandidateId, rf.VotedFor)
	}

	reply.VoteGranted = false
	rf.debug(LRPC, "<- RequestVote Denied")
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	EntryTerms   []int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	EarlierTermIndex int
}

func (rf *Raft) AppendEntries(
	args AppendEntriesArgs,
	reply *AppendEntriesReply) {
	// Your code here.
	rf.lock()
	defer rf.unlock()

	rf.debug(LRPC, "-> AppendEntries %v", args)

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		rf.debug(LAE, "Rejecting AE due to sender old term (mine %v, theirs %v)",
			reply.Term, args.Term)
		reply.Success = false
		reply.EarlierTermIndex = len(rf.LogTerms)
		rf.debug(LRPC, "<- AppendEntries %v", *reply)
		return
	}

	if rf.CurrentTerm < args.Term {
		rf.debug(LRV, "My term is stale; stepping down as leader.")
		rf.stepDown(args.Term)
	}

	rf.lastHeartbeat = time.Now()

	nextIndex := args.PrevLogIndex + 1

	if args.PrevLogIndex >= len(rf.LogTerms) {
		rf.debug(LAE, "Rejecting AE due to no such prev index (%v) in log",
			args.PrevLogIndex)
		rf.dumpLog()
		// No match!
		reply.Success = false
		reply.EarlierTermIndex = len(rf.LogTerms)
		rf.debug(LRPC, "<- AppendEntries %v", *reply)
		return
	}

	if rf.LogTerms[args.PrevLogIndex] != args.PrevLogTerm {
		rf.debug(LAE, "Rejecting AE due to non-matching prev term "+
			"(index %v, args term %v, found term %v) in log",
			args.PrevLogIndex, args.PrevLogTerm, rf.LogTerms[args.PrevLogIndex])
		myTerm := rf.LogTerms[args.PrevLogIndex]
		for i := args.PrevLogIndex; i > 0; i-- {
			if myTerm != rf.LogTerms[i] {
				break
			}
			reply.EarlierTermIndex = i
		}
		// Long enough, but wrong stuff!
		reply.Success = false
		rf.debug(LRPC, "<- AppendEntries %v", *reply)
		return
	}

	// Now we know prev matches.

	// Check to see if we need to truncate anything.
	rf.dumpLog()
	for i := 0; i < len(args.Entries); i++ {
		idx := nextIndex + i
		entryTerm := args.EntryTerms[i]
		entry := args.Entries[i]
		if len(rf.LogTerms) == idx {
			// New entry would extend the log; no problem.
			rf.LogTerms = append(rf.LogTerms, entryTerm)
			rf.Log = append(rf.Log, entry)
		} else if entryTerm != rf.LogTerms[idx] {
			// New entry conflicts with old one, throw away log tail and extend.
			rf.debug(LAE, "Throwing away log entries to replace with ones from leader")
			rf.LogTerms = rf.LogTerms[:idx]
			rf.Log = rf.Log[:idx]
			rf.LogTerms = append(rf.LogTerms, entryTerm)
			rf.Log = append(rf.Log, entry)
		} else {
			// New entry matches old one, nothing to do.
		}
	}
	rf.dumpLog()

	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		n := args.LeaderCommit
		if nextIndex < n {
			n = nextIndex
		}
		rf.commitIndex = n
		rf.debug(LAE, "COMMIT INDEX from leader now %v", n)
	}

	//rf.debug("AE OK")
	reply.Success = true
	rf.LeaderId = args.LeaderId
	rf.debug(LRPC, "<- AppendEntries %v", *reply)
	return
}

func (rf *Raft) stepDown(newTerm int) {
	rf.CurrentTerm = newTerm
	rf.VotedFor = -1
	rf.isLeader = false
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.persist()
}

func (rf *Raft) stepUp() {
	rf.isLeader = true
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.Log)
	}
	const appendNilEntryOnElection = false
	if appendNilEntryOnElection {
		rf.LogTerms = append(rf.LogTerms, rf.CurrentTerm)
		rf.Log = append(rf.Log, nil)
	}
	rf.persist()
	rf.doAppendEntries()
	rf.lastHeartbeat = time.Now()
	rf.debug(LRV, "LEADER in term %v with log:", rf.CurrentTerm)
	rf.dumpLog()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int) bool {
	rf.lock()
	myLogIndex := len(rf.Log) - 1
	myLogTerm := rf.LogTerms[len(rf.Log)-1]
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, myLogIndex, myLogTerm}
	rf.unlock()

	// Need to do retransmits? At least until we "win"?
	// Shouldn't need to; timeouts will handle the issue.
	var reply RequestVoteReply
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		rf.lock()
		if reply.Term > rf.CurrentTerm {
			rf.debug(LRV, "My term is stale; stepping down as leader.")
			rf.stepDown(reply.Term)
		}
		rf.unlock()
		return reply.VoteGranted
	} else {
		return false
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.lock()
	if !rf.isLeader {
		rf.unlock()
		return
	}
	sendingTerm := rf.CurrentTerm

	nextIndex := rf.nextIndex[server]
	prevLogIndex := nextIndex - 1
	var entryTerms []int
	var entries []interface{}

	prevLogTerm := rf.LogTerms[prevLogIndex]

	if len(rf.Log) > nextIndex {
		entries = rf.Log[nextIndex:]
		entryTerms = rf.LogTerms[nextIndex:]
	}

	args := AppendEntriesArgs{sendingTerm, rf.me, prevLogIndex,
		prevLogTerm, entryTerms, entries, rf.commitIndex}
	rf.unlock()

	done := make(chan bool)
	go func() {
		var reply AppendEntriesReply
		//rf.debug("%v -> %v AE %v", rf.me, server, *args)
		ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
		//rf.debug("%v <- %v AE ok=%v %v", rf.me, server, ok, reply)
		if ok {
			rf.lock()
			if reply.Term > rf.CurrentTerm {
				rf.debug(LRV, "My term is stale; stepping down as leader.")
				rf.stepDown(reply.Term)
			} else if !rf.isLeader || sendingTerm != rf.CurrentTerm {
				// Do nothing. We aren't leader anymore.
			} else {
				if reply.Success {
					if len(args.Entries) > 0 {
						rf.replicationProgress = true
						mi := nextIndex + len(args.Entries) - 1
						if mi > rf.matchIndex[server] {
							rf.matchIndex[server] = mi
						}
						ni := nextIndex + len(args.Entries)
						if ni > rf.nextIndex[server] {
							rf.nextIndex[server] = ni
						}
						rf.debug(LAE, "Server %v nextIndex %v matchIndex %v",
							server, rf.nextIndex[server], rf.matchIndex[server])
					}
				} else {
					rf.replicationProgress = true
					const useOptimization = true
					if useOptimization {
						rf.debug(LAE, "Lowering nextIndex for %v to %v",
							server, reply.EarlierTermIndex)
						rf.nextIndex[server] = reply.EarlierTermIndex
					} else {
						rf.debug(LAE, "Lowering nextIndex for %v to %v", server, nextIndex-1)
						rf.nextIndex[server] = nextIndex - 1
					}
				}
			}
			rf.unlock()
		} else {
			rf.debug(LRPC, "LOST AE to %v", server)
		}
		done <- true
	}()

	go func() {
		<-done

		rf.lock()
		if rf.isLeader {
			for ciToTry := rf.commitIndex + 1; ciToTry < len(rf.Log); ciToTry++ {
				nOk := len(rf.peers)
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me && rf.matchIndex[i] < ciToTry {
						nOk--
					}
				}
				if nOk >= rf.nMajority {
					if rf.LogTerms[ciToTry] == rf.CurrentTerm || nOk == len(rf.peers) {
						rf.commitIndex = ciToTry
						rf.debug(LAE, "COMMIT INDEX %v", ciToTry)
					} else {
						rf.debug(LAE, "Wanted to advance COMMIT INDEX to %v "+
							"but ending log term (%v) is not currentTerm (%v)",
							ciToTry, rf.LogTerms[len(rf.LogTerms)-1], rf.CurrentTerm)
					}
				}
			}
		}
		rf.unlock()
	}()
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.debug(LIFACE, "-> START %v", command)
	rf.lock()
	defer rf.unlock()

	if !rf.isLeader {
		rf.debug(LIFACE, "<- START %v %v %v %v", command, -1, -1, false)
		return -1, -1, false
	}

	index := len(rf.Log)
	term := rf.CurrentTerm
	isLeader := true

	rf.LogTerms = append(rf.LogTerms, rf.CurrentTerm)
	rf.Log = append(rf.Log, command)
	rf.persist()

	rf.debug(LIFACE, "<- START %v %v %v %v", command, index, term, isLeader)
	rf.dumpLog()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.lock()
	defer rf.unlock()
	rf.killed = true
}

func (rf *Raft) becomeCandidate() {
	// NOTE Cannot hold lock during this whole thing since we need to handle RPCs
	// while running the election.

	timeoutChan := time.After(pickTimeout())

	rf.lock()
	rf.CurrentTerm++
	candidacyTerm := rf.CurrentTerm
	rf.VotedFor = rf.me
	rf.persist()
	rf.unlock()

	rf.debug(LRV, "CANDIDATE term %v", candidacyTerm)

	votes := make(chan bool)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			votes <- rf.sendRequestVote(peer)
		}(peer)
	}

	oks := make(map[int]bool)
	//notOks := make(map[int]bool)

	oks[rf.me] = true
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		ok := false
		//breakOut := false
		select {
		case ok = <-votes:
			break
		case <-timeoutChan:
			rf.debug(LRV, "<- CANDIDATE TIMEOUT")
			return
			//breakOut = true
			//break
		}

		//if breakOut {
		//break
		//}

		rf.lock()
		currentTerm := rf.CurrentTerm
		rf.unlock()
		if candidacyTerm != currentTerm {
			rf.debug(LRV, "<- CANDIDATE STALE TERM")
			return
			//break
		}

		if ok {
			oks[peer] = true
			rf.debug(LRV, "Got ok vote back: now at %v votes", len(oks))
		} // else {
		//notOks[peer] = true
		//}
		if len(oks) >= rf.nMajority {
			rf.lock()
			if candidacyTerm != rf.CurrentTerm {
				rf.debug(LRV, "<- CANDIDATE STALE TERM")
				rf.unlock()
				//break
				return
			}
			rf.stepUp()
			rf.lastHeartbeat = time.Now()
			rf.unlock()
			//break
			return
		}
	}

	// Make sure we receive every response or RPC thread won't exit.
	//for len(oks)+len(notOks) < len(rf.peers) {
	//<-votes
	//}

	// If we get here our election failed, just go back to being follower.
}

func (rf *Raft) doAppendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.sendAppendEntries(peer)
		}(peer)
	}
}

func (rf *Raft) runTimer() {
	timeoutMs := pickTimeout()
	for {
		delay := 50 * time.Millisecond * slowdown

		rf.lock()
		// Kludge to deal with my naive approach to timers. Without this tweak
		// candidates might caravan.
		tilTimeout := time.Now().Sub(rf.lastHeartbeat)
		if !rf.isLeader && tilTimeout < timeoutMs {
			delay = tilTimeout
		}

		// Tweak that can accelarate replication is replication seems to be making
		// progress. DON'T TURN THIS ON. It seems to lead to livelock.
		if rf.replicationProgress {
			//delay = 2 * time.Millisecond * slowdown
			rf.replicationProgress = false
			//rf.debug(LAE, "Accelerating replication...")
		}
		rf.unlock()

		time.Sleep(delay)

		rf.lock()
		if rf.killed {
			rf.unlock()
			return
		}

		if rf.isLeader {
			rf.doAppendEntries()
			rf.lastHeartbeat = time.Now()
		} else {
			lastHeartbeat := rf.lastHeartbeat
			if time.Since(lastHeartbeat) > timeoutMs {
				rf.unlock()
				rf.debug(LRV, "Last heard from leader %v ago", time.Since(lastHeartbeat))
				rf.becomeCandidate()
				rf.lock()
				rf.lastHeartbeat = time.Now()
				timeoutMs = pickTimeout()
			}
		}
		rf.unlock()
	}
}

func pickTimeout() time.Duration {
	electionTimeout := 150

	timeoutMs :=
		time.Duration(rand.Intn(electionTimeout)+electionTimeout) * time.Millisecond
	timeoutMs *= slowdown

	return timeoutMs
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
	rf.applyCh = applyCh

	// Your initialization code here.
	rf.nMajority = len(rf.peers)/2 + 1
	rf.VotedFor = -1
	rf.lastHeartbeat = time.Now()

	rf.Log = make([]interface{}, 1)
	rf.LogTerms = make([]int, 1)

	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.LeaderId = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runTimer()
	go rf.doApplications()

	return rf
}

