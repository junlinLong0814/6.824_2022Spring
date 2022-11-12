package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const TIME_OUT = 100

const (
	CGet    = "Get"
	CPut    = "Put"
	CAppend = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command string
	Key     string
	Value   string
	ClerkId int64
	CmdSeq  int64
	ReqId   int64
}

type OpResult struct {
	Err    Err
	Result string
	CmdSeq int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data       map[string]string
	Req2Result map[int64]chan OpResult

	//just save the latest result
	clerkLatestSeq map[int64]int64
	clerkLatestRes map[int64]OpResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//If it is a duplicate log,
	//return it immediately to avoid too many duplicate logs in the raft layer

	clerk := args.Clerk

	kv.mu.Lock()
	if latestSeq, ok := kv.clerkLatestSeq[clerk]; ok {
		if latestSeq == args.CmdSeq {
			reply.Err = kv.clerkLatestRes[clerk].Err
			reply.Value = kv.clerkLatestRes[clerk].Result
			kv.mu.Unlock()
			return
		} else if latestSeq > args.CmdSeq {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	command := Op{
		Command: CGet,
		Key:     args.Key,
		ClerkId: args.Clerk,
		CmdSeq:  args.CmdSeq,
		ReqId:   nrand(),
	}

	if _, _, isleader := kv.rf.Start(command); isleader {
		clerkResChan := make(chan OpResult, 1)
		kv.mu.Lock()
		kv.Req2Result[command.ReqId] = clerkResChan
		kv.mu.Unlock()
		//LogInfo("[%d] think is a leader!\n", kv.me)
		select {
		case opResult := <-clerkResChan:
			reply.Err = opResult.Err
			reply.Value = opResult.Result
		case <-time.After(TIME_OUT * time.Millisecond):
			//time out
			reply.Err = ErrTimeOut
		}
		kv.mu.Lock()
		delete(kv.Req2Result, command.ReqId)
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	//If it is a duplicate log,
	//return it immediately to avoid too many duplicate logs in the raft layer
	clerk := args.Clerk

	kv.mu.Lock()
	if latestSeq, ok := kv.clerkLatestSeq[clerk]; ok {
		if latestSeq == args.CmdSeq {
			reply.Err = kv.clerkLatestRes[clerk].Err
			kv.mu.Unlock()
			return
		} else if latestSeq > args.CmdSeq {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	command := Op{
		Command: args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.Clerk,
		CmdSeq:  args.CmdSeq,
		ReqId:   nrand(),
	}

	if _, _, isleader := kv.rf.Start(command); isleader {
		clerkResChan := make(chan OpResult, 1)
		kv.mu.Lock()
		kv.Req2Result[command.ReqId] = clerkResChan
		kv.mu.Unlock()
		//LogInfo("[%d] think is a leader!\n", kv.me)
		select {
		case opResult := <-clerkResChan:
			reply.Err = opResult.Err
		case <-time.After(TIME_OUT * time.Millisecond):
			//time out
			reply.Err = ErrTimeOut
		}
		kv.mu.Lock()
		delete(kv.Req2Result, command.ReqId)
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) update(clerk int64, latestSeq int64, latestRes OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.clerkLatestSeq[clerk] = latestSeq
	kv.clerkLatestRes[clerk] = latestRes
}

func (kv *KVServer) applyRes2Chan(reqId int64, result OpResult) {
	kv.mu.Lock()
	reqChan, ok := kv.Req2Result[reqId]
	kv.mu.Unlock()
	if ok {
		reqChan <- result
	}
}

// check whether need to process
func (kv *KVServer) check_dup(op Op) (bool, OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	client_last_process_seq, ok := kv.clerkLatestSeq[op.ClerkId]
	if ok {
		if op.CmdSeq <= client_last_process_seq {
			// need not to process
			return false, kv.clerkLatestRes[op.ClerkId]
		}
	}
	// need to process
	return true, OpResult{}
}

func (kv *KVServer) run() {
	//for !kv.killed() {
	for perCommand := range kv.applyCh {
		//check command is valid or not
		if perCommand.CommandValid {
			op := perCommand.Command.(Op)
			//check command repeate or not
			need_process, res := kv.check_dup(op)
			if !need_process {
				kv.applyRes2Chan(op.ReqId, res)
				continue
			}
			result := OpResult{
				CmdSeq: op.CmdSeq,
				Err:    OK,
				Result: "",
			}
			switch op.Command {
			case CGet:
				if val, ok := kv.data[op.Key]; ok {
					result.Result = val
				} else {
					result.Result = ""
					result.Err = ErrNoKey
				}
				kv.update(op.ClerkId, op.CmdSeq, result)
			case CPut:
				kv.data[op.Key] = op.Value
				kv.update(op.ClerkId, op.CmdSeq, result)
			case CAppend:
				if val, ok := kv.data[op.Key]; ok {
					kv.data[op.Key] = val + op.Value
				} else {
					kv.data[op.Key] = op.Value
				}
				kv.update(op.ClerkId, op.CmdSeq, result)
			}
			kv.applyRes2Chan(op.ReqId, result)
		}
	}
	//}

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.Req2Result = make(map[int64]chan OpResult)
	kv.clerkLatestRes = make(map[int64]OpResult)
	kv.clerkLatestSeq = make(map[int64]int64)

	// You may need initialization code here.
	go kv.run()

	return kv
}
