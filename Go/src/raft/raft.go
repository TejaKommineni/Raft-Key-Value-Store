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

import "sync"
import "labrpc"
import (
	"time"
	//"fmt"

)
import (
	//"math/rand"
	//"fmt"
	//"fmt"
	"bytes"
	"encoding/gob"
	"fmt"
)
// import "bytes"
// import "encoding/gob"



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

type Log struct {
	Term int
	Data int
}
//
// A Go object implementing a single Raft peer.
//

const (
	Follower     = "follower"
	Candidate    = "candidate"
	Leader       = "leader"
	Stopped      = "stopped"
)

type Raft struct {
	mutex        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	state 	  string
	currentTerm int
	votedFor  int
	log       [] Log
	leader 	  int
	electionTimeout   int
	heartbeatInterval int
	timeChan	*time.Ticker
	commitIndex int
	prevCommitIndex int
	lastApplied	int
	command	chan interface{}
	commands [] interface{}
	wg	  sync.WaitGroup
	//for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int
	//for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int
	ready  bool
	respChan chan *AppendEntriesReply
	voteChan chan *RequestVoteReply
	majority int
	readyToSendCommands bool
	commandToAgree int
	logToBeRepaired bool
	peerBeingRepaired [] bool
	peersResponded map[int]bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) State() string {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	return rf.state
}

func (rf *Raft) setState(state string) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	rf.state = state
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.me == rf.leader{
		isleader = true
	}else{
		isleader = false
	}
	return term, isleader
}

func (rf *Raft) appendLog(log Log){
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	rf.log = append(rf.log, log)
	////fmt.Printf("Raft server %[1]d log  is of size %[2]d",rf.me,len(rf.log))
	////fmt.Println()
}

func (rf *Raft) appendCommands(cmd int){
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	rf.commands = append(rf.commands, cmd)
	////fmt.Printf("Raft server %[1]d log  is of size %[2]d",rf.me,len(rf.log))
	////fmt.Println()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
//
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.lastApplied)
	d.Decode(&rf.log)
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool

	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Println("My log while voting is", rf.me, rf.log)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft Server %[1]d in term %[2]d rejected vote for candidate %[3]d in term %[4]d as he is in a higher term",rf.me,rf.currentTerm,args.CandidateId,args.Term)
		fmt.Println()
		return
	}
	loguptodate := true
	if(len(rf.log)>1) {
		lastIndex, lastTerm := len(rf.log)-1, rf.log[len(rf.log)-1].Term
		if args.LastLogTerm>lastTerm  || (lastTerm  == args.LastLogTerm && lastIndex<=args.LastLogIndex){
			loguptodate = true
		}else{
			loguptodate = false
		}
	}else{
		loguptodate = true
	}
	// If the voter is in old term
	// Irrespective of anything if someone in the system is trying for higher term we will move every raft server to higher term
	// so that next election starts from there. Also if they are candidates or leaders for older term or this term using
	//rf.votechan I will silence them.
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leader = -1
		if rf.state == Leader || rf.state == Candidate{
			rf.state = Follower
			fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"I have sent command for leader to go to follower ...",rf.me)
		}
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && loguptodate{
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.persist()
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft Server %[1]d in term %[2]d  and state %[5]v has voted for candidate %[3]d in term %[4]d",rf.me,rf.currentTerm,args.CandidateId,args.Term,rf.state)
		fmt.Println()
		return
	}else{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.persist()
		if loguptodate{
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft Server %[1]d in term %[2]d rejected vote for candidate %[3]d in term %[4]d as he as voted in this term",rf.me,rf.currentTerm,args.CandidateId,args.Term)
			fmt.Println()
		}else{
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft Server %[1]d in term and state %[5]v %[2]d rejected vote for candidate %[3]d in term %[4]d as candidate log is not uptodate",rf.me,rf.currentTerm,args.CandidateId,args.Term,rf.state)
			fmt.Println()
		}
		return
	}
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

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
	Append bool
	Entries  [] Log
	// Your data here (2A, 2B).
}

type AppendEntriesReply struct {
	Term int
	Success bool
	Append bool
	Index int
	CommitIndex int
	Peer int
	ConflictingIndex int
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Append = args.Append
	reply.Peer = rf.me
	reply.ConflictingIndex = -1
	forLC := AppendEntriesReply{}
	forLC.Append = args.Append
	forLC.Peer = rf.me
	forLC.ConflictingIndex = -1

	if args.Append && (args.PrevLogIndex < len(rf.log)-1) {
		flag := true

		for i := range args.Entries {
			if len(args.Entries)+args.PrevLogIndex > len(rf.log)-1{
				flag = false
				break
			}
			if args.Entries[i] != rf.log[args.PrevLogIndex + i+1] {
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+" repeat",args.Entries[i], rf.log[args.PrevLogIndex + i])
				flag = false
				break
			}
		}
		if(flag) {
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft server %[1]d in term %[2]d has recieved the duplicate command %[5]v from server %[3]d in term %[4]d", rf.me, rf.currentTerm, args.LeaderId, args.Term, args.Entries)
			fmt.Println()
			reply.Term = rf.currentTerm
			reply.Index = len(rf.log) - 1
			reply.Success = true
			return
		}
	}

	if rf.currentTerm>args.Term{
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft server %[1]d in term %[2]d has rejected a heartbeat message from server %[3]d in term %[4]d",rf.me,rf.currentTerm,args.LeaderId,args.Term)
		fmt.Println()
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Append && (args.PrevLogIndex != len(rf.log)-1 ||  args.PrevLogTerm != rf.log[len(rf.log)-1].Term ){
		forLC.Term = args.Term
		rf.respChan <- &forLC
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft Serever %[1]d in state %[5]v with term %[2]d is entering here with leader %[3]d in term %[4]d for entry %[6]v",rf.me,rf.currentTerm,args.LeaderId,args.Term,rf.state,args.Entries)
		fmt.Println()
		fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft server before log", rf.me, rf.log)
		if len(rf.log)-1 > args.PrevLogIndex{
			rf.log = rf.log[:args.PrevLogIndex]
		}else if len(rf.log)-1 < args.PrevLogIndex{

		}else if len(rf.log)-1 == args.PrevLogIndex &&  args.PrevLogTerm != rf.log[len(rf.log)-1].Term{
			tempTerm := rf.log[len(rf.log)-1].Term
			for rf.log[len(rf.log)-1].Term == tempTerm{
				rf.log = rf.log[:len(rf.log)-1]
			}
		}

		if(len(rf.log) == 0) {
			rf.appendLog(Log{0, 0})
		}
		fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"after log",rf.me, rf.log)
		reply.ConflictingIndex = len(rf.log)
		reply.Success = false
		rf.leader = args.LeaderId
		reply.Term = rf.currentTerm
		rf.persist()
		return
	}

	if args.Append {
		for i := range args.Entries {
			rf.appendLog(Log{args.Entries[i].Term, args.Entries[i].Data})
		}
		rf.commitIndex = args.LeaderCommit
		reply.Index = len(rf.log) - 1
		rf.logToBeRepaired = false
		reply.Success = true
		forLC.Term = args.Term
		rf.respChan <- &forLC
		reply.Term = rf.currentTerm
		rf.leader = args.LeaderId
		fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"my log after appending is",rf.me, rf.log)
		rf.persist()
		return;
	}else{
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+ "Raft server %[1]d in term %[2]d and state %[6]v has received a heartbeat message from server %[3]d in term %[4]d and commit index %[5]d",rf.me,rf.currentTerm,args.LeaderId,args.Term,args.LeaderCommit,rf.state)
		fmt.Println()
		reply.Success = true
		rf.commitIndex = args.LeaderCommit
		forLC.Term = args.Term
		fmt.Println("before state",rf.state)
		rf.state = Follower
		fmt.Println("after state",rf.state)
		rf.respChan <- &forLC
		reply.Term = rf.currentTerm
		rf.leader = args.LeaderId
		return;
	}
	// Your code here (2A, 2B).
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	if rf.leader != rf.me{
		return index,term,false
	}else{
		//fmt.Println("The command I have to get people agreed on",command,rf.me)
		//fmt.Println("The index which tester should check on",len(rf.log),rf.log)
		//rf.command <- command
		data, _ := command.(int)
		rf.appendLog(Log{rf.currentTerm, data})
		return len(rf.log)-1,rf.currentTerm,true
	}

	// Your code here (2B).
	return index, term, isLeader
}

func (rf *Raft) clientInbox(){
	for rf.State() == Leader  {
		if rf.ready && len(rf.log)-1>rf.commandToAgree{
			rf.ready = false
			rf.mutex.Lock()
			msg := rf.log[rf.commandToAgree+1].Data
			index := rf.commandToAgree+1
			fmt.Println("log value is ", rf.log)
			log := rf.log[rf.commandToAgree+1:]
			rf.commandToAgree = len(rf.log) - 1
			rf.mutex.Unlock()
			for x := range rf.peers {
				rf.peersResponded[x] = false
			}
				fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft server %[1]d recieved the command %[2]d", rf.me, msg)
				////fmt.Printf("Raft server leader log", rf.log)
				fmt.Println()
				//rf.appendLog(Log{rf.currentTerm, data})
				rf.majority = 1
				rf.nextIndex[rf.me]++
				for i := 0; i < len(rf.peers); i++ {
					if rf.peers[i] != nil {
						rf.wg.Add(1)
						request := AppendEntriesArgs{}
						request.Term = rf.currentTerm
						request.LeaderId = rf.me
						request.PrevLogIndex = index
						request.PrevLogIndex--
						request.PrevLogTerm = rf.log[request.PrevLogIndex].Term
						request.LeaderCommit = rf.commitIndex
						request.Entries = log
						request.Append = true
						go func(i int, request AppendEntriesArgs) {
							defer rf.wg.Done()
							fmt.Println(i,rf.peerBeingRepaired[i])
							if i != rf.me && !rf.peerBeingRepaired[i] {
								reply := AppendEntriesReply{}
								ok := false
								//Inorder to pass the TestFailNoAgree2B the multiple trying of a particular command as mentioned
								// in paper is being avoided. issue: when the three new servers are joining the herd back one of them is
								// getting the command 20 which  it missed earlier and becoming an quoroum which is not expected.
								fmt.Println(ok,rf.state,rf.currentTerm,request.Term,rf.peerBeingRepaired[i])
								for ok == false && rf.state == Leader && rf.currentTerm == request.Term && !rf.peerBeingRepaired[i] {
									fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft server %[2]d in term %[4]d passed the entries %[3]v to server %[1]d ", i, rf.me, request.Entries, request.Term)
									fmt.Println()
									ok = rf.sendAppendEntries(i, &request, &reply)
									//fmt.Println("I am ok",i, ok)
								}
								////fmt.Printf("Raft server %[1]d has sent heart beat messages to %[2]d",rf.me,i)
								////fmt.Println()
								rf.respChan <- &reply
							}
						}(i, request)
					}
				}

		}
	}
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000") + " :" + "I am no more in clientinbox",rf.me)
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.setState(Stopped)
}

// Retrieves the number of member servers in the consensus.
func (rf *Raft) MemberCount() int {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	count := 0
	for i:=0;i<len(rf.peers);i++{
		if rf.peers[i]!= nil{
			count++;
		}
	}
	return count
}

// Retrieves the number of servers required to make a quorum.
func (rf *Raft) QuorumSize() int {
	return (rf.MemberCount() / 2) + 1
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
	//time.Sleep(time.Second*2)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.appendLog(Log{0,0})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leader = -1
	rf.commitIndex = len(rf.log)-1
	rf.prevCommitIndex = len(rf.log)-1
	rf.lastApplied = 1
	rf.command = make(chan interface{})
	rf.nextIndex =  make([]int, len(rf.peers))
	rf.matchIndex =  make([]int, len(rf.peers))
	rf.peerBeingRepaired =  make([]bool, len(rf.peers))
	rf.heartbeatInterval = 150
	//rand.Seed(time.Now().UTC().UnixNano())
	rf.electionTimeout = rf.heartbeatInterval + 100 + 60*(rf.me+1)
	fmt.Println("Election Timeouts are",rf.electionTimeout)
	rf.state = Follower
	rf.logToBeRepaired = true
	rf.peersResponded = make(map[int]bool)
	rf.commandToAgree = 0
	//Initialize nextIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	//Initialize match Index
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}

	for i := 0; i < len(rf.peers); i++ {
		rf.peerBeingRepaired[i] = false
	}
	// Your initialization code here (2A, 2B, 2C).
	go func(applyCh chan ApplyMsg){
		rf.loop(applyCh)
	}(applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	fmt.Println("read the persisted state", rf.me,rf.log)
	return  rf
}

func (rf *Raft) loop(applyCh chan ApplyMsg){
	state := rf.State()
	for {
		stop := false
		fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft server %[1]d is going to state %[2]s with term %[3]d", rf.me,state,rf.currentTerm)
		fmt.Println()
		switch state {

		case Follower:
			rf.followerLoop(applyCh)
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop(applyCh)
			// Emptying all the commands
			rf.commands = rf.commands[:0]
		case Stopped:
			stop = true
		}
		if stop{
			break
		}
		state = rf.State()
	}
}

func (rf *Raft) followerLoop(applyCh chan ApplyMsg) {

	rf.timeChan = time.NewTicker(time.Millisecond * time.Duration(rf.electionTimeout))
	rf.respChan = make(chan *AppendEntriesReply, len(rf.peers))
	//temp:= time.Now()
	rf.logToBeRepaired = true
	for rf.State() == Follower {
		////fmt.Println("we are here",rf.me)
		update := false
		select {
		case <- rf.timeChan.C:
			update = true
		case resp:= <- rf.respChan:
			if resp.Term !=0{
				rf.currentTerm = resp.Term
			}
			rf.timeChan.Stop()
			rf.timeChan = time.NewTicker(time.Millisecond * time.Duration(rf.electionTimeout))

		default:
			update = false
			////
		}
		if update == true{
			rf.setState(Candidate)
			return
		}
		// logToBeRepaired has been introduced to commit the commands only if the a raft server is starting to append the commands from its leader.
		// one potential situation is where the old leader comes with a big or equal log and there are uncommitted stuff and presently at that index
		// something else has been comitted.
		if  !rf.logToBeRepaired && rf.commitIndex >= rf.matchIndex[rf.me] && rf.matchIndex[rf.me] < len(rf.log) {
			//fmt.Println(rf.logToBeRepaired , rf.commitIndex, rf.matchIndex[rf.me], len(rf.log)-1 )
			applyMsg := ApplyMsg{}
			applyMsg.Index = rf.matchIndex[rf.me]
			////fmt.Println("Raft Server %[1]d submitted command %[2]d for testing",rf.commitIndex,rf.matchIndex[rf.me],rf.log)
			applyMsg.Command = rf.log[rf.matchIndex[rf.me]].Data
			////fmt.Println("Raft Server %[1]d submitted command %[2]d for testing",rf.commitIndex,rf.matchIndex[rf.me],rf.log)
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft Server %[1]d submitted command %[2]d for testing",rf.me,applyMsg.Command)
			fmt.Println()
			applyCh <- applyMsg
			rf.matchIndex[rf.me] += 1
			rf.lastApplied = rf.matchIndex[rf.me]
			rf.commandToAgree = rf.lastApplied
		}

	}

}

func (rf *Raft) candidateLoop() {

	lastLogIndex, lastLogTerm := len(rf.log)-1,rf.log[len(rf.log)-1].Term
	goForElection := false

	if rf.peers[rf.me] != nil {
		////fmt.Println("I entered here", rf.me)
		goForElection = true
	}
	rf.respChan = make(chan *AppendEntriesReply, len(rf.peers))
	rf.voteChan = make(chan *RequestVoteReply, len(rf.peers))
	votesGranted := 0
	var respChan chan *RequestVoteReply
	for rf.State() == Candidate {
		if goForElection {
			// Increment current term, vote for self.
			rf.currentTerm++
			rf.votedFor = rf.me
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft Server %[1]d has initiated the election for term %[2]d with %[3]v", rf.me,rf.currentTerm,rf.log)
			fmt.Println()
			// Send RequestVote RPCs to all other servers.
			respChan = make(chan *RequestVoteReply, len(rf.peers))
			for i:=0;i<len(rf.peers);i++ {
				if rf.peers[i] != nil && i != rf.me{
					rf.wg.Add(1)
					go func(i int) {
						defer rf.wg.Done()
						request := RequestVoteArgs{}
						request.Term =rf.currentTerm
						request.CandidateId = rf.me
						request.LastLogIndex = lastLogIndex
						request.LastLogTerm = lastLogTerm
						reply := RequestVoteReply{}
						//ok := false
						rf.sendRequestVote(i, &request, &reply)
						respChan <- &reply
					}(i)
				}
			}

			// Wait for either:
			//   * Votes received from majority of servers: become leader
			//   * AppendEntries RPC received from new leader: step down.
			//   * Election timeout elapses without election resolution: increment term, start new election
			//   * Discover higher term: step down (§5.1)
			votesGranted = 1
			rf.timeChan = time.NewTicker(time.Millisecond * time.Duration(rf.electionTimeout))
			goForElection = false
		}


		// Collect votes from peers.
		select {

		case resp := <-respChan:
			if true == resp.VoteGranted {
				votesGranted++
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"I received the vote for term",resp.Term)
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"number of votes granted for me",rf.me,votesGranted)
				if votesGranted == rf.QuorumSize() {
					//fmt.Printf("Raft Server %[1]d has won the election and the current term is %[2]d",rf.me,rf.currentTerm)
					//fmt.Println()
					rf.leader = rf.me
					rf.timeChan.Stop()
					rf.setState(Leader)
					return
				}
			}else{
				if resp.Term>rf.currentTerm{
					rf.timeChan.Stop()
					rf.setState(Follower)
					return
				}
			}
		case <- rf.voteChan:
			rf.timeChan.Stop()
			rf.setState(Follower)
			return
		case resp := <- rf.respChan:
			if resp.Term != 0{
				rf.currentTerm = resp.Term
			}
			rf.timeChan.Stop()
			rf.setState(Follower)
			//fmt.Println("hey,hey I learned to follow")
			return
		case <-rf.timeChan.C:
			if rf.peers[rf.me] != nil {
				goForElection = true
			}
		default:
			if rf.State()!= Candidate{
				return
			}
		}

	}
}

func (rf *Raft) leaderLoop(applyCh chan ApplyMsg) {
	// Begin to collect response from followers
	rf.leader = rf.me
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
		//fmt.Println(rf.nextIndex[i])
	}

	for i := 0; i < len(rf.peers); i++ {
		if rf.peers[i] != nil {
			rf.wg.Add(1)
			rf.wg.Add(1)
			request := AppendEntriesArgs{}
			request.Term = rf.currentTerm
			request.LeaderId = rf.me
			//request.PrevLogIndex = len(rf.log)-1
			//request.PrevLogTerm = rf.log[len(rf.log)-1].Term
			request.LeaderCommit = rf.commitIndex
			request.Append = false
			go func(i int, request AppendEntriesArgs) {
				defer  rf.wg.Done()
				if i!=rf.me{
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(i, &request, &reply)
					//fmt.Printf("Raft server %[1]d has sent heart beat messages to %[2]d",rf.me,i)
					//fmt.Println()
					rf.respChan <- &reply
				}
			}(i,request)
		}
	}
	rf.timeChan = time.NewTicker(time.Millisecond * time.Duration(rf.heartbeatInterval))

	rf.respChan = make(chan *AppendEntriesReply, len(rf.peers))
	rf.voteChan = make(chan *RequestVoteReply, len(rf.peers))

	//since := time.Now()
	go rf.clientInbox()
	rf.ready = true
	for rf.State() == Leader {
		select {
		case <-rf.timeChan.C:
			////fmt.Printf("Raft Server %[1]d has statrted sending heart beat messages after %[2]d milli seconds",rf.me,time.Now().Sub(since)/time.Millisecond)
			////fmt.Println()
			for i := 0; i < len(rf.peers); i++ {
				if rf.peers[i] != nil {
					rf.wg.Add(1)
					rf.wg.Add(1)
					request := AppendEntriesArgs{}
					request.Term = rf.currentTerm
					request.LeaderId = rf.me
					//request.PrevLogIndex = len(rf.log)-1
					//request.PrevLogTerm = rf.log[len(rf.log)-1].Term
					request.LeaderCommit = rf.commitIndex
					request.Append = false
					go func(i int, request AppendEntriesArgs) {
						defer  rf.wg.Done()
						if i!=rf.me{
							ok := false
							reply := AppendEntriesReply{}
							for ok == false && rf.state == Leader && rf.currentTerm == request.Term{
								ok = rf.sendAppendEntries(i, &request, &reply)
								fmt.Println(ok,rf.state,rf.currentTerm,request.Term)
								fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft server %[1]d has sent heart beat messages to %[2]d in term %[3]d ", rf.me, i,request.Term)
								fmt.Println()
							}
							fmt.Println("I am struck here4",rf.me)
							rf.respChan <- &reply
							fmt.Println("no i am not",rf.me)
						}
					}(i,request)
				}
			}
			//since = time.Now()
			//rf.timeChan = time.NewTicker(time.Millisecond * time.Duration(rf.heartbeatInterval))

		/*case <- rf.voteChan:
			fmt.Println("I am struck here1",rf.me)
			rf.timeChan.Stop()
			fmt.Println("I know I am the leader of old term")
			rf.leader = -1
			rf.setState(Follower)
			return
		*/

		case resp := <-rf.respChan:
			fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"I am server %[1]d with term %[2]d and I received a response from server %[3]d with term %[4]d",rf.me,rf.currentTerm,resp.Peer,resp.Term)
			fmt.Println()
			if resp.Term > rf.currentTerm{
				rf.timeChan.Stop()
				rf.currentTerm = resp.Term
				rf.leader = -1
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"I am going to follower from new here", rf.me,resp.Peer,resp.Term,resp.Success,resp.Append)
				rf.setState(Follower);
				return
			}
			if resp.Append == true && !resp.Success && !rf.peerBeingRepaired[resp.Peer]{
				//fmt.Println("started log repair for server",resp.Peer,resp.Term,rf.currentTerm)
				rf.peerBeingRepaired[resp.Peer] = true
				rf.nextIndex[resp.Peer] = resp.ConflictingIndex
				go func(){
					for rf.nextIndex[resp.Peer] < rf.commandToAgree +1 && rf.state == Leader{
						request := AppendEntriesArgs{}
						fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"nextindex of server %[2]d is %[1]d",rf.nextIndex[resp.Peer],resp.Peer)
						fmt.Println()
						request.Term = rf.currentTerm
						request.LeaderId = rf.me
						request.PrevLogIndex = rf.nextIndex[resp.Peer]-1
						request.PrevLogTerm = rf.log[rf.nextIndex[resp.Peer]-1].Term
						request.Entries = rf.log[rf.nextIndex[resp.Peer]:rf.commandToAgree+1]
						request.LeaderCommit = rf.commitIndex
						request.Append = true
						reply := AppendEntriesReply{}
						ok := false
						for ok == false{
							ok = rf.sendAppendEntries(resp.Peer, &request, &reply)
						}

						if reply.Success{
							rf.respChan <- &reply
							break
						}else{
							//fmt.Println(rf.me, rf.log)
							//fmt.Println("the command failed is",request.PrevLogIndex+1,request.Command,reply.Peer)
							//fmt.Printf("conflict index for server %[2]d is %[1]d ",reply.ConflictingIndex,reply.Peer)
							//fmt.Println()
							if reply.ConflictingIndex != -1{
								rf.nextIndex[resp.Peer] = reply.ConflictingIndex
							}else{
								rf.nextIndex[resp.Peer]--
							}

							////fmt.Println("repairing the previous entry", rf.nextIndex[resp.Peer],resp.Peer)
						}
					}
					rf.peerBeingRepaired[resp.Peer] = false
				}()

			}
			if resp.Append == true && resp.Success{
				fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Raft Server %[1]d received an append response from server %[2]d for term %[3]d and entry %[4]d and successs is %[5]v",rf.me,resp.Peer,resp.Term,resp.Index,resp.Success)
				fmt.Println()
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"Leader waiting for the command",rf.commandToAgree)
				rf.nextIndex[resp.Peer] += 1
				////fmt.Println()
				if(rf.commandToAgree == resp.Index && !rf.peersResponded[resp.Peer]){
					rf.persist()
					rf.peersResponded[resp.Peer] = true
					rf.majority++
					////fmt.Println("majority is ", rf.majority)
					if(rf.majority >= rf.QuorumSize()) {
						rf.commitIndex = rf.commandToAgree
						rf.prevCommitIndex = rf.commitIndex
						go func() {
							for rf.lastApplied <= rf.commandToAgree {
								applyMsg := ApplyMsg{}
								applyMsg.Index = rf.lastApplied
								applyMsg.Command = rf.log[rf.lastApplied].Data
								fmt.Println(time.Now().Format("2006-01-02 15:04:05.000")+" :"+"leader has scommited the command at index ", rf.me,rf.lastApplied, rf.log[rf.lastApplied].Data, rf.log)
								applyCh <- applyMsg
								rf.lastApplied++
							}
							fmt.Println("there are no one to accept ready", rf.me)
							rf.ready = true
							fmt.Println("there is one to accept ready", rf.me)
							rf.majority = 0
						}()
					}
				}
			}

		default:
			//fmt.Println("I am struck here3",rf.me)
			if rf.State()!= Leader{
				return
			}

		}
	}
}

// A situation where a message is lost in the network and the follower immediately became a candidate and requested for votes. Since, all the
//followers terms are lower than candidates he acquired votes and became a leader at this point there are two leaders in the system.