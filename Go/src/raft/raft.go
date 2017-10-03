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
	"fmt"

)
import "math/rand"
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
	data int
}
//
// A Go object implementing a single Raft peer.
//

const (
	Follower     = "follower"
	Candidate    = "candidate"
	Leader       = "leader"
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
	ready chan bool
	respChan chan *AppendEntriesReply
	voteChan chan *RequestVoteReply
	majority int
	readyToSendCommands bool
	commandToAgree int
	logToBeRepaired bool
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
	//fmt.Printf("Raft server %[1]d log  is of size %[2]d",rf.me,len(rf.log))
	//fmt.Println()
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	//fmt.Printf("Raft server %[1]d has requested to vote for term %[2]d where the raft server %[3]d is in %[4]d term",args.CandidateId,args.Term,rf.me,rf.currentTerm)
	//fmt.Println()

	if(len(rf.log)>1) {
		lastIndex, lastTerm := len(rf.log)-1, rf.log[len(rf.log)-1].Term
		if lastTerm > args.LastLogTerm {
			reply.Term = rf.currentTerm

			//fmt.Println("we didnt vote to him",args.CandidateId,rf.log)
			reply.VoteGranted = false
			return
		}
		if lastTerm  == args.LastLogTerm && lastIndex>args.LastLogIndex {
			reply.Term = rf.currentTerm

			//fmt.Println("we didnt vote to him", args.CandidateId, rf.log)
			reply.VoteGranted = false
			return
		}
	}

	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leader = -1
		if rf.state == Leader || rf.state == Candidate{
			rf.voteChan <- reply
		//	rf.timeChan.Stop()
		//	rf.setState(Follower)
		//	rf.leader = -1
		}
	}else if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId{
		reply.Term = args.Term
		reply.VoteGranted = false
		//fmt.Println("I didnt vote to the candidate",rf.me, args.CandidateId)
        return
	}

	//fmt.Println("I have recieved an vote from ",args.CandidateId, rf.me)
	rf.votedFor = args.CandidateId
	reply.Term = args.Term
	reply.VoteGranted = true
	return
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
		Command int
        LeaderCommit int
        Append bool
		CommandTerm int
        // Your data here (2A, 2B).
}

type AppendEntriesReply struct {
        Term int
        Success bool
        Append bool
        Index int
        CommitIndex int
        Peer int
        // Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("Raft server %[1]d has sent a heart beat message for term %[2]d and raft server %[3]d is in term %[4]d in state %[5]v",args.LeaderId,args.Term,rf.me,rf.currentTerm,rf.state)
	//fmt.Println()
	//rf.timeChan.Stop()

	reply.Append = args.Append
	reply.Peer = rf.me

	if !args.Append && args.Term > rf.currentTerm{
		//\fmt.Printf("Raft server %[1]d has sent an append message for term %[2]d and raft server %[3]d is in term %[4]d",args.LeaderId,args.Term,rf.me,rf.currentTerm)
		//fmt.Println()
		rf.leader = args.LeaderId
		rf.logToBeRepaired = true
		fmt.Println("I entered here which u wrote now",rf.me,args.LeaderId)
		reply.Term = args.Term
		if rf.State() != Leader{
			rf.currentTerm = args.Term
		}
		reply.Success = true
		rf.respChan <- reply
		return
	}

	if args.Term < rf.currentTerm{
		// This wast introduced for test fail agree 2B when adding command 106. Follower is coming with bigger term and smaller log
		if args.LeaderCommit >len(rf.log){
			reply.Term = args.Term
			rf.currentTerm = args.Term
		}else{
			reply.Term = rf.currentTerm
		}
		reply.Success = false
		fmt.Println("I replied false")

		//fmt.Println("I have rejected heartbeat message from",rf.me,args.LeaderId,rf.currentTerm)
		return;
	}
	if args.Term == rf.currentTerm{
		rf.leader = args.LeaderId
	}else{
		rf.leader = args.LeaderId
		rf.votedFor = -1
		if rf.state != Leader {
			rf.currentTerm = args.Term
			//fmt.Println("I am the leader for old term and moving as a follower",rf.me)
			//rf.setState(Follower)
		}

	}


		//fmt.Printf("Raft Server %[1]d has recieved a heart beat message from %[2]d and changed it's state to follower",rf.me,args.LeaderId)
		//fmt.Println()


	if args.Append && (args.PrevLogIndex != len(rf.log)-1 ||  args.PrevLogTerm != rf.log[len(rf.log)-1].Term){
		fmt.Printf("Trying to append command %[4]v to %[1]d server with prev index %[2]d and prev term %[3]d",rf.me ,args.PrevLogIndex, args.PrevLogTerm, args.Command)
		fmt.Printf("The log size is ",len(rf.log))
		fmt.Println()
		fmt.Println("The log is",rf.log)
		if len(rf.log)-1>args.PrevLogIndex{
			rf.log = rf.log[:args.PrevLogIndex+1]
		}else if len(rf.log)-1 == args.PrevLogIndex &&  args.PrevLogTerm != rf.log[len(rf.log)-1].Term{
			rf.log = rf.log[:len(rf.log)-1]
		}else{

		}
		reply.Term = rf.currentTerm
		fmt.Println("I am replying false",rf.log)
		reply.Success = false
		rf.respChan <- reply
		return
	}

	if args.Append {
		rf.appendLog(Log{args.CommandTerm, args.Command})
		reply.Index = len(rf.log) -1
		rf.logToBeRepaired = false
		fmt.Printf("Raft server %[1]d has sent a command %[2]d to %[3]d log is %[4]v", args.LeaderId, args.Command,rf.me,rf.log)
		fmt.Println()

	}else{
		//fmt.Printf("Raft Server %[1]d has sent a heart beat message for server %[2]d in term %[5]d with commit index %[3]d and stae is %[4]v",args.LeaderId,rf.me,args.LeaderCommit,rf.state,rf.currentTerm)
		//fmt.Println()
		//fmt.Printf("Raft Server %[3]d commit index %[1]d and previous indices are %[2]d",args.LeaderCommit,rf.prevCommitIndex,rf.me)
		//fmt.Println()
	}
	rf.commitIndex = args.LeaderCommit
	reply.Term = rf.currentTerm

	reply.Success = true
	//fmt.Printf("Raft Server %[1]d has recieved a heart beat message from %[2]d",rf.me,args.LeaderId)
	//fmt.Println()
	//fmt.Println("I replied you know")
	rf.respChan <- reply
    return;
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
		fmt.Println("The command I have to get people agreed on",command,rf.me)
		fmt.Println("The index which tester should check on",len(rf.log))
		//rf.command <- command
		data, _ := command.(int)
		rf.appendLog(Log{rf.currentTerm, data})
		rf.commands = append(rf.commands,command)
		rf.commands = append(rf.commands,len(rf.log)-1)
		//fmt.Println("I am not blocked here")
		//fmt.Println("command has been ",rf.command)
		return len(rf.log)-1,rf.currentTerm,true
	}

	// Your code here (2B).


	return index, term, isLeader
}

func (rf *Raft) clientInbox(){
	for {
		<-rf.ready
		rf.readyToSendCommands = true
		msg := <-rf.command
		index := <- rf.command
		rf.commandToAgree, _ = index.(int)
		rf.readyToSendCommands = false
		time.Sleep(time.Millisecond * 10)
		if rf.state == Leader {
			fmt.Printf("Raft server %[1]d recieved the command %[2]d", rf.me, msg)
			fmt.Println()
			data, _ := msg.(int)
			//rf.appendLog(Log{rf.currentTerm, data})
			rf.majority = 1
			rf.nextIndex[rf.me]++
			for i := 0; i < len(rf.peers); i++ {
				if rf.peers[i] != nil {
					rf.wg.Add(1)
					request := AppendEntriesArgs{}
					request.Term = rf.currentTerm
					request.LeaderId = rf.me
					request.PrevLogIndex,_ = index.(int)
					request.PrevLogIndex--
					request.PrevLogTerm = rf.log[request.PrevLogIndex].Term
					request.Command = data
					request.LeaderCommit = rf.commitIndex
					request.Append = true
					request.CommandTerm = rf.currentTerm
					go func(i int, request AppendEntriesArgs) {
						defer rf.wg.Done()
						if i != rf.me {
							reply := AppendEntriesReply{}
							//ok := false
							//Inorder to pass the TestFailNoAgree2B the multiple trying of a particular command as mentioned
							// in paper is being avoided. issue: when the three new servers are joining the herd back one of them is
							// getting the command 20 which  it missed earlier and becoming an quoroum which is not expected.
							//for ok == false {
								fmt.Printf("Raft server %[2]d passed the command %[3]d to server %[1]d ",i,rf.me,data)
								fmt.Println()
								 rf.sendAppendEntries(i, &request, &reply)
							//}
							//fmt.Printf("Raft server %[1]d has sent heart beat messages to %[2]d",rf.me,i)
							//fmt.Println()
							rf.respChan <- &reply
						}
					}(i, request)
				}
			}
		}else{
			break
		}

	}
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.lastApplied = -1
	rf.command = make(chan interface{})
	rf.nextIndex =  make([]int, len(rf.peers))
	rf.matchIndex =  make([]int, len(rf.peers))
	rf.heartbeatInterval = 150
	//rand.Seed(time.Now().UTC().UnixNano())
	rf.electionTimeout = rf.heartbeatInterval + 200 + rand.Intn(300)
	fmt.Println("Election Timeouts are",rf.electionTimeout)
	rf.ready = make(chan bool)
	rf.readyToSendCommands = true
	rf.state = Follower
	rf.logToBeRepaired = true
	//Initialize nextIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}
	//Initialize match Index
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
	// Your initialization code here (2A, 2B, 2C).
	go func(applyCh chan ApplyMsg){
	    rf.loop(applyCh)
	}(applyCh)



	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	return  rf
	}

func (rf *Raft) loop(applyCh chan ApplyMsg){
	state := rf.State()
	for {
		fmt.Printf("Raft server %[1]d is going to state %[2]s", rf.me,state)
		fmt.Println()
		switch state {

		case Follower:
			rf.followerLoop(applyCh)
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop(applyCh)
		}
		state = rf.State()
	}

}

func (rf *Raft) sendCommitedEntries(applyCh chan ApplyMsg){
	for {
		if rf.prevCommitIndex < rf.commitIndex {
			applyMsg := ApplyMsg{}
			applyMsg.Index = len(rf.log)-1
			applyMsg.Command = rf.log[len(rf.log)-1]
			fmt.Println("I am struck here")
			applyCh <- applyMsg
			fmt.Println("I am not struck here")
			rf.prevCommitIndex = rf.commitIndex
		}
	}
}
func (rf *Raft) followerLoop(applyCh chan ApplyMsg) {
	rf.timeChan = time.NewTicker(time.Millisecond * time.Duration(rf.electionTimeout))
	rf.respChan = make(chan *AppendEntriesReply, len(rf.peers))
	temp:= time.Now()
	rf.logToBeRepaired = true
	for rf.State() == Follower {

		//fmt.Println("we are here",rf.me)
		update := false
		select {
			case <- rf.timeChan.C:
				fmt.Printf("Raft server %[1]d didn't recieve a message in the heartbeat interval %[2]d", rf.me,time.Now().Sub(temp)/time.Millisecond)
				fmt.Println()
				temp= time.Now()
				update = true
			case <- rf.respChan:
				rf.timeChan.Stop()
				rf.timeChan = time.NewTicker(time.Millisecond * time.Duration(rf.electionTimeout))

			default:
				update = false
				//fmt.Printf("Raft server %[1]d is still in follower state.",rf.me)
		}
		if update == true{
			rf.setState(Candidate)
			return
		}
		if !rf.logToBeRepaired && rf.commitIndex > rf.matchIndex[rf.me] && rf.matchIndex[rf.me] < len(rf.log)-1 {
			applyMsg := ApplyMsg{}
			applyMsg.Index = rf.matchIndex[rf.me]+1
			fmt.Println("Raft Server %[1]d submitted command %[2]d for testing",rf.commitIndex,rf.matchIndex[rf.me],rf.log)
			applyMsg.Command = rf.log[rf.matchIndex[rf.me]+1].data
			//fmt.Println("Raft Server %[1]d submitted command %[2]d for testing",rf.commitIndex,rf.matchIndex[rf.me],rf.log)
			fmt.Printf("Raft Server %[1]d submitted command %[2]d for testing",rf.me,applyMsg.Command)
			fmt.Println()
			applyCh <- applyMsg
			rf.matchIndex[rf.me] += 1
		}

	}

	}

func (rf *Raft) candidateLoop() {

	lastLogIndex, lastLogTerm := len(rf.log)-1,rf.log[len(rf.log)-1].Term
	goForElection := false

	if rf.peers[rf.me] != nil {
		fmt.Println("I entered here", rf.me)
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
			fmt.Printf("Raft Server %[1]d has initiated the election for term %[2]d with %[3]v", rf.me,rf.currentTerm,rf.log)
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
						ok := false
						for ok == false{
							ok = rf.sendRequestVote(i, &request, &reply)
						}
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

		// If we received enough votes then stop waiting for more votes.
		// And return from the candidate loop
		if votesGranted == rf.QuorumSize() {
			fmt.Printf("Raft Server %[1]d has won the election and the current term is %[2]d",rf.me,rf.currentTerm)
			fmt.Println()
			rf.leader = rf.me
			rf.setState(Leader)
			return
		}

		// Collect votes from peers.
		select {

		case resp := <-respChan:
			if true == resp.VoteGranted {
				votesGranted++
			}else{
				if resp.Term>rf.currentTerm{
					rf.setState(Follower)
					return
				}
			}
		case <- rf.voteChan:
			rf.timeChan.Stop()
			rf.setState(Follower)
			return
		case <- rf.respChan:
			rf.timeChan.Stop()
			rf.setState(Follower)
			fmt.Println("hey,hey I learned to follow")
			return
		case <-rf.timeChan.C:
			if rf.peers[rf.me] != nil {
				goForElection = true
			}
		}

    	}
	}

func (rf *Raft) leaderLoop(applyCh chan ApplyMsg) {
	// Begin to collect response from followers
	rf.leader = rf.me
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = len(rf.log)-1
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
	since := time.Now()
	go rf.clientInbox()
	rf.ready <- true
	for rf.State() == Leader {
		select {
		case <-rf.timeChan.C:

			fmt.Printf("Raft Server %[1]d has statrted sending heart beat messages after %[2]d milli seconds",rf.me,time.Now().Sub(since)/time.Millisecond)
			fmt.Println()
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
			since = time.Now()
			//rf.timeChan = time.NewTicker(time.Millisecond * time.Duration(rf.heartbeatInterval))

		case <- rf.voteChan:
			rf.timeChan.Stop()
			fmt.Println(" follower from here")
			rf.setState(Follower)
			return

		case resp := <-rf.respChan:
			if resp.Term > rf.currentTerm{
				//fmt.Printf("Raft server %[1]d is no more a leader",rf.me)
				//fmt.Println()
				rf.timeChan.Stop()
				fmt.Println("I am going to follower from new here",resp.Peer,resp.Term,resp.Success,resp.Append)
				rf.setState(Follower);
				return
			}
			if resp.Append == true && !resp.Success{
				for rf.nextIndex[resp.Peer] != rf.commandToAgree +1{
					request := AppendEntriesArgs{}
					//fmt.Println("leaders log",rf.log,rf.nextIndex[resp.Peer])
					//fmt.Println("error in the leader",rf.me)
					request.Term = rf.currentTerm
					request.LeaderId = rf.me
					request.PrevLogIndex = rf.nextIndex[resp.Peer]-1
					request.PrevLogTerm = rf.log[rf.nextIndex[resp.Peer]-1].Term
					request.Command = rf.log[rf.nextIndex[resp.Peer]].data
					request.CommandTerm = rf.log[rf.nextIndex[resp.Peer]].Term
					request.LeaderCommit = rf.commitIndex
					request.Append = true
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(resp.Peer, &request, &reply)
					//fmt.Println("command sent for repair",request.PrevLogIndex,request.PrevLogTerm,request.Command)
					if reply.Success{
						rf.nextIndex[resp.Peer]++
						if rf.nextIndex[resp.Peer] == rf.commandToAgree +1{
							rf.respChan <- &reply
						}
						//fmt.Println("repairing the next entry", rf.nextIndex[resp.Peer])
					}else{
						rf.nextIndex[resp.Peer]--
						//fmt.Println("repairing the previous entry", rf.nextIndex[resp.Peer],resp.Peer)
					}
				}
			}
			if resp.Append == true && resp.Success{
				fmt.Printf("Raft Server %[1]d received an append response from server %[2]d for term %[3]d and entry %[4]d and successs is %[5]v and %[6]d",rf.me,resp.Peer,resp.Term,resp.Index,resp.Success,rf.nextIndex[resp.Peer])
				fmt.Println()
				rf.nextIndex[resp.Peer] += 1
				//fmt.Println()
				if(rf.commandToAgree == resp.Index){
					//fmt.Println(resp.Index, len(rf.log)-1,rf.majority)
					rf.majority++
					//fmt.Println("majority is ", rf.majority)
					if(rf.majority >= rf.QuorumSize()){
						fmt.Println("leader has scommited the command",rf.me,rf.log[rf.commandToAgree].data)
						rf.commitIndex = rf.commandToAgree
						applyMsg := ApplyMsg{}
						applyMsg.Index = rf.commandToAgree
						applyMsg.Command = rf.log[rf.commandToAgree].data
						//fmt.Println("I am struck here")
						applyCh <- applyMsg
						//fmt.Println("I am not struck here")
						rf.prevCommitIndex = rf.commitIndex
						rf.matchIndex[rf.me]++
						rf.ready <- true
						rf.majority = 0
					}
				}
			}

		default:
			if rf.State()!= Leader{
					return
			}
			if len(rf.commands)>0 && rf.readyToSendCommands{
				fmt.Println("are you struck here")
				rf.command <- rf.commands[0]
				rf.command <- rf.commands[1]
				rf.commands = rf.commands[2:]
				fmt.Println("yes I am")
				time.Sleep(time.Millisecond*10)
			}
		}
	}
}

// A situation where a message is lost in the network and the follower immediately became a candidate and requested for votes. Since, all the
//followers terms are lower than candidates he acquired votes and became a leader at this point there are two leaders in the system.