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
	Command interface{}
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
	timeChan	*time.Timer
	wg	  sync.WaitGroup
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
	if rf.currentTerm > args.Term{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == Leader{
			rf.timeChan.Stop()
			rf.setState(Follower)
		}
        }else if rf.votedFor != -1 && rf.votedFor != args.CandidateId{
		reply.Term = args.Term
		reply.VoteGranted = false
		//fmt.Println("I didnt vote to the candidate",rf.me, args.CandidateId)
        return
	}

	//lastIndex, lastTerm := rf.len(log)-1,rf.log[len(rf.log)-1].Term
	//if lastIndex > args.LastLogIndex || lastTerm >args.LastLogTerm{
	//	*reply.Term = rf.currentTerm
        //        *reply.VoteGranted = false
        //        return nil
	//}
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
	
        // Your data here (2A, 2B).
}

type AppendEntriesReply struct {
        Term int
        Success bool
        // Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("Raft server %[1]d has sent a heart beat message for term %[2]d and raft server %[3]d is in term %[4]d",args.LeaderId,args.Term,rf.me,rf.currentTerm)
	//fmt.Println()
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		//fmt.Println("I have rejected heartbeat message from",rf.me,args.LeaderId,rf.currentTerm)
		return;
	}
	if args.Term == rf.currentTerm{
		rf.leader = args.LeaderId
	}else{
		rf.leader = args.LeaderId
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == Leader {
			//fmt.Println("I am the leader for old term and moving as a follower",rf.me)
			rf.timeChan.Stop()
			rf.setState(Follower)
		}
	}
	if rf.state == Candidate {
		// change state to follower
		rf.timeChan.Stop()
		rf.setState(Follower)
		//fmt.Printf("Raft Server %[1]d has recieved a heart beat message from %[2]d and changed it's state to follower",rf.me,args.LeaderId)
		//fmt.Println()
	}
	rf.timeChan.Stop()
	rf.timeChan = time.NewTimer(time.Millisecond * time.Duration(rf.electionTimeout))

	reply.Term = rf.currentTerm
	reply.Success = true
	//fmt.Printf("Raft Server %[1]d has recieved a heart beat message from %[2]d",rf.me,args.LeaderId)
	//fmt.Println()
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

	// Your code here (2B).


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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leader = -1
	rf.heartbeatInterval = 150
	rf.electionTimeout = rf.heartbeatInterval + 200 + rand.Intn(100)
	rf.state = Follower
	// Your initialization code here (2A, 2B, 2C).
	go func(){
	    rf.loop()
	}()
	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())
	return  rf
	}

func (rf *Raft) loop(){
	state := rf.State()
	for {
		//fmt.Printf("Raft server %[1]d is going to state %[2]s", rf.me,state)
		//fmt.Println()
		switch state {

		case Follower:
			rf.followerLoop()
		case Candidate:
			rf.candidateLoop()
		case Leader:
			rf.leaderLoop()
		}
		state = rf.State()
	}

}

func (rf *Raft) followerLoop() {
	rf.timeChan = time.NewTimer(time.Millisecond * time.Duration(rf.electionTimeout))
	//temp:= time.Now()
	for rf.State() == Follower {

		//fmt.Println()
		update := false
		select {

			case <- rf.timeChan.C:
				//fmt.Printf("Raft server %[1]d didn't recieve a message in the heartbeat interval %[2]d", rf.me,time.Now().Sub(temp))
				//fmt.Println()
				//temp= time.Now()
				update = true
			default:
				update = false
				//fmt.Printf("Raft server %[1]d is still in follower state.",rf.me)
		}
		if update == true{
			rf.setState(Candidate)
		}
	}
}

func (rf *Raft) candidateLoop() {

	lastLogIndex, lastLogTerm := 0,rf.currentTerm
	goForElection := true
	votesGranted := 0
	var respChan chan *RequestVoteReply

	for rf.State() == Candidate {
		if goForElection {
			// Increment current term, vote for self.
			rf.currentTerm++
			rf.votedFor = rf.me
			//fmt.Printf("Raft Server %[1]d has initiated the election", rf.me)
			//fmt.Println()
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
			rf.timeChan = time.NewTimer(time.Millisecond * time.Duration(rf.electionTimeout))
			goForElection = false
		}

		// If we received enough votes then stop waiting for more votes.
		// And return from the candidate loop
		if votesGranted == rf.QuorumSize() {
			//fmt.Printf("Raft Server %[1]d has won the election and the current term is %[2]d",rf.me,rf.currentTerm)
			//fmt.Println()
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

		case <-rf.timeChan.C:
			goForElection = true
		}
	}
}

func (rf *Raft) leaderLoop() {
	// Begin to collect response from followers
	rf.timeChan = time.NewTimer(time.Millisecond * time.Duration(1))
	var respChan chan *AppendEntriesReply
	rf.leader = rf.me
	for rf.State() == Leader {
		select {
		case resp := <-respChan:
			if resp.Term > rf.currentTerm{
				//fmt.Printf("Raft server %[1]d is no more a leader",rf.me)
				//fmt.Println()
				rf.setState(Follower);
				return
			}
		case <-rf.timeChan.C:
			respChan = make(chan *AppendEntriesReply, len(rf.peers))
			//fmt.Printf("Raft Server %[1]d has statrted sending heart beat messages after %[2]d milli seconds",rf.me,time.Now().Sub(since))
			//fmt.Println()
			for i := 0; i < len(rf.peers); i++ {
				if rf.peers[i] != nil {
					rf.wg.Add(1)
					go func(i int) {
						defer  rf.wg.Done()
						if i!=rf.me{
							request := AppendEntriesArgs{}
							request.Term = rf.currentTerm
							request.LeaderId = rf.me
							request.PrevLogIndex = 0
							request.PrevLogTerm = 0
							reply := AppendEntriesReply{}
							rf.sendAppendEntries(i, &request, &reply)
							//fmt.Printf("Raft server %[1]d has sent heart beat messages to %[2]d",rf.me,i)
							//fmt.Println()
							respChan <- &reply
						}
					}(i)
				}
			}
			//since = time.Now()
			rf.timeChan = time.NewTimer(time.Millisecond * time.Duration(rf.heartbeatInterval))

		default:
			if rf.State()!= Leader{
					return
			}
		}
	}
}

// A situation where a message is lost in the network and the follower immediately became a candidate and requested for votes. Since, all the
//followers terms are lower than candidates he acquired votes and became a leader at this point there are two leaders in the system.
