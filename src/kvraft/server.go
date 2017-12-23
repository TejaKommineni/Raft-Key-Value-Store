package raftkv

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
	"time"
	//"fmt"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	OpIdentifier uint64
	Key string
	Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	killed bool					 		// To verify if kill method is called on a key value server.
	OperationExecuted map[uint64]bool	// To verify if an operation is already executed.
	GetMap map[uint64]string	 		// Map that stores results for each OpIdentifier.
	State map[string]string  	 		// The map that state machine manages

}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// If Key Value Server is Killed.
	if kv.killed {
		reply.WrongLeader = true
		reply.Leader = -1
		return
	}

	//If this operation is already executed.
	kv.mu.Lock()
	_ , ok := kv.OperationExecuted[args.OpIdentifier]
	kv.mu.Unlock()
	if ok {
		kv.mu.Lock()
		reply.Value = kv.GetMap[args.OpIdentifier]
		kv.mu.Unlock()
		reply.Err = OK
		return
	}

	var Operation Op
	Operation.OpType = "Get"
	Operation.Key = args.Key
	Operation.OpIdentifier = args.OpIdentifier

	kv.mu.Lock()
	_, _, Leader := kv.rf.Start(Operation)
	kv.mu.Unlock()

	// // If this Key Value Server is not a leader.
	if !Leader {
		reply.WrongLeader = true
		reply.Leader = kv.rf.LeaderId
		return
	}

	_, Leader = kv.rf.GetState()
	for !kv.killed && Leader{
		kv.mu.Lock()
		_, ok := kv.OperationExecuted[Operation.OpIdentifier]
		kv.mu.Unlock()
		if ok {
			kv.mu.Lock()
			reply.Value = kv.GetMap[Operation.OpIdentifier]
			kv.mu.Unlock()
			reply.Err = OK
			break
		}
		time.Sleep(10 * time.Millisecond)
		_, Leader = kv.rf.GetState()
	}
	
	if kv.killed ||  !Leader {
		reply.WrongLeader = true
		if kv.killed {
			reply.Leader = -1
		} else {
			reply.Leader = kv.rf.LeaderId
		}
		return
	}
	return 
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//If Key Value Server is Killed.
	if kv.killed {
		reply.WrongLeader = true
		reply.Leader = -1
		return
	}

	//If this operation is already executed.
	kv.mu.Lock()
	_ , ok := kv.OperationExecuted[args.OpIdentifier]
	kv.mu.Unlock()
	if ok {
		reply.Err = OK
		return
	}

	var Operation Op
	Operation.OpType = args.Op
	Operation.Key = args.Key
	Operation.Value = args.Value
	Operation.OpIdentifier = args.OpIdentifier

	kv.mu.Lock()
	_, _, Leader := kv.rf.Start(Operation)
	kv.mu.Unlock()

	// If this Key Value Server is not a leader.
	if !Leader {
		reply.WrongLeader = true
		reply.Leader = kv.rf.LeaderId
		return
	}

	_, Leader = kv.rf.GetState()
	for !kv.killed && Leader {
		kv.mu.Lock()
		_, ok := kv.OperationExecuted[Operation.OpIdentifier]
		kv.mu.Unlock()
		if ok {
			reply.Err = OK
			break
		}
		time.Sleep(10 * time.Millisecond)
		_, Leader = kv.rf.GetState()
	}
	
	if kv.killed || !Leader {
		reply.WrongLeader = true
		if kv.killed {
			reply.Leader = -1
		} else {
			reply.Leader = kv.rf.LeaderId
		}
		return
	}

	return 
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killed = true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.GetMap = make(map[uint64]string)
	kv.OperationExecuted = make(map[uint64]bool)
	kv.State = make(map[string]string)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killed = false
	go kv.ApplyChannelLoop()

	return kv
}

func (kv *RaftKV) ApplyChannelLoop() {
	for applyMsg := range kv.applyCh {

		if kv.killed {
			return 
		}
		Operation := ((applyMsg.Command).(Op))
		kv.mu.Lock()
		_, ok := kv.OperationExecuted[Operation.OpIdentifier]
		kv.mu.Unlock()
		if ok == true {
			continue
		}

		if Operation.OpType == "Put" {
			kv.mu.Lock()
			kv.State[Operation.Key] = Operation.Value
			kv.mu.Unlock()
		} else if Operation.OpType == "Append" {
			kv.mu.Lock()
			kv.State[Operation.Key] += Operation.Value
			kv.mu.Unlock()
		} else {
			kv.mu.Lock()
			value, ok := kv.State[Operation.Key]
			kv.mu.Unlock()
			kv.mu.Lock()
			if ok {
				kv.GetMap[Operation.OpIdentifier] = value
			} else {
				kv.GetMap[Operation.OpIdentifier] = ""
			}
			kv.mu.Unlock()
		}
		kv.mu.Lock()
		kv.OperationExecuted[Operation.OpIdentifier] = true
		kv.mu.Unlock()
	}
}


