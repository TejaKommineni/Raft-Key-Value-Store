package raftkv


import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"


var mu sync.Mutex
var clientId uint64

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	Id uint64
	mu sync.Mutex
	PreviousLeader int	
	Counter uint64
	
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) GenerateCounter() uint64{
	Id := ck.Counter
	ck.Counter++
	return Id
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.PreviousLeader = int((uint64(nrand()) % uint64(len(ck.servers))))
	mu.Lock()
	ck.Id = clientId
	clientId++
	mu.Unlock()
	ck.Counter = ck.Id << 32
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	var args GetArgs
	var result string  = ""
	var ok bool = false
	args.Key = key
	ck.mu.Lock()
	args.OpIdentifier = ck.GenerateCounter()
	ck.mu.Unlock()
	for !ok{
		reply := new(GetReply)
		ok = ck.servers[ck.PreviousLeader].Call("RaftKV.Get", &args, reply)
		if ok {
			if reply.WrongLeader {
				ok = false
				ck.PreviousLeader = int((uint64(nrand()) % uint64(len(ck.servers))))
				continue
			} else {
				result = reply.Value
				break
			}
		} else {
			ck.PreviousLeader = int((uint64(nrand()) % uint64(len(ck.servers))))
		}
	}
	return result
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	ck.mu.Lock()
	args.OpIdentifier = ck.GenerateCounter()
	ck.mu.Unlock()
	ok := false
	for !ok{
		reply := new(PutAppendReply)
		ok = ck.servers[ck.PreviousLeader].Call("RaftKV.PutAppend", &args, reply)
		if ok {
			if reply.WrongLeader {
				ok = false
				ck.PreviousLeader = int((uint64(nrand()) % uint64(len(ck.servers))))
				continue
			} else {
				break
			}
		} else {
			ck.PreviousLeader = int((uint64(nrand()) % uint64(len(ck.servers))))
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}