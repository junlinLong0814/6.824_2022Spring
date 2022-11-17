package shardctrler

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type CmdType string

const (
	QUERY    CmdType = "QUERY"
	JOIN     CmdType = "JOIN"
	LEAVE    CmdType = "LEAVE"
	MOVE     CmdType = "MOVE"
	TIME_OUT         = 100 // set time out to 100 millsecond.
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	Req2Result     map[int64]chan OpResult
	clerkLatestSeq map[int64]int64
	clerkLatestRes map[int64]OpResult
}

type Op struct {
	// Your data here.
	Type    CmdType
	GIDs    []int
	Shard   int
	Num     int
	Servers map[int][]string

	ReqId   int64
	ClerkId int64
	CmdSeq  int64
}

type OpResult struct {
	Config Config
	Error  Err
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	clerk := args.ClerkId

	sc.mu.Lock()
	if latestSeq, ok := sc.clerkLatestSeq[clerk]; ok {
		if latestSeq == args.Cmdseq {
			reply.Err = sc.clerkLatestRes[clerk].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if latestSeq > args.Cmdseq {
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:    JOIN,
		Servers: args.Servers,
		ClerkId: clerk,
		CmdSeq:  args.Cmdseq,
		ReqId:   nrand(),
	}

	if _, _, ok := sc.rf.Start(op); ok {
		tmpChan := make(chan OpResult, 1)
		sc.mu.Lock()
		sc.Req2Result[op.ReqId] = tmpChan
		sc.mu.Unlock()

		select {
		case opResult := <-tmpChan:
			reply.Err = opResult.Error
			reply.WrongLeader = false
		case <-time.After(TIME_OUT * time.Millisecond):
			//time out
			reply.Err = TIMEOUT
			reply.WrongLeader = true
		}

		sc.mu.Lock()
		delete(sc.Req2Result, op.ReqId)
		sc.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	clerk := args.ClerkId

	sc.mu.Lock()
	if latestSeq, ok := sc.clerkLatestSeq[clerk]; ok {
		if latestSeq == args.Cmdseq {
			reply.Err = sc.clerkLatestRes[clerk].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if latestSeq > args.Cmdseq {
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:    LEAVE,
		GIDs:    args.GIDs,
		ClerkId: clerk,
		CmdSeq:  args.Cmdseq,
		ReqId:   nrand(),
	}

	if _, _, ok := sc.rf.Start(op); ok {
		tmpChan := make(chan OpResult, 1)
		sc.mu.Lock()
		sc.Req2Result[op.ReqId] = tmpChan
		sc.mu.Unlock()

		select {
		case opResult := <-tmpChan:
			reply.Err = opResult.Error
			reply.WrongLeader = false
		case <-time.After(TIME_OUT * time.Millisecond):
			//time out
			reply.Err = TIMEOUT
			reply.WrongLeader = true
		}

		sc.mu.Lock()
		delete(sc.Req2Result, op.ReqId)
		sc.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	clerk := args.ClerkId

	sc.mu.Lock()
	if latestSeq, ok := sc.clerkLatestSeq[clerk]; ok {
		if latestSeq == args.Cmdseq {
			reply.Err = sc.clerkLatestRes[clerk].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if latestSeq > args.Cmdseq {
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:    MOVE,
		Shard:   args.Shard,
		GIDs:    make([]int, 1),
		ClerkId: clerk,
		CmdSeq:  args.Cmdseq,
		ReqId:   nrand(),
	}
	op.GIDs[0] = args.GID

	if _, _, ok := sc.rf.Start(op); ok {
		tmpChan := make(chan OpResult, 1)
		sc.mu.Lock()
		sc.Req2Result[op.ReqId] = tmpChan
		sc.mu.Unlock()

		select {
		case opResult := <-tmpChan:
			reply.Err = opResult.Error
			reply.WrongLeader = false
		case <-time.After(TIME_OUT * time.Millisecond):
			//time out
			reply.Err = TIMEOUT
			reply.WrongLeader = true
		}

		sc.mu.Lock()
		delete(sc.Req2Result, op.ReqId)
		sc.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	clerk := args.ClerkId

	sc.mu.Lock()
	if latestSeq, ok := sc.clerkLatestSeq[clerk]; ok {
		if latestSeq == args.Cmdseq {
			reply.Err = sc.clerkLatestRes[clerk].Error
			reply.WrongLeader = false
			sc.mu.Unlock()
			return
		} else if latestSeq > args.Cmdseq {
			sc.mu.Unlock()
			return
		}
	}
	sc.mu.Unlock()

	op := Op{
		Type:    QUERY,
		Num:     args.Num,
		ClerkId: clerk,
		CmdSeq:  args.Cmdseq,
		ReqId:   nrand(),
	}

	if _, _, ok := sc.rf.Start(op); ok {
		tmpChan := make(chan OpResult, 1)
		sc.mu.Lock()
		sc.Req2Result[op.ReqId] = tmpChan
		sc.mu.Unlock()

		select {
		case opResult := <-tmpChan:
			reply.Err = opResult.Error
			reply.WrongLeader = false
			reply.Config = opResult.Config
		case <-time.After(TIME_OUT * time.Millisecond):
			//time out
			reply.Err = TIMEOUT
			reply.WrongLeader = true
		}

		sc.mu.Lock()
		delete(sc.Req2Result, op.ReqId)
		sc.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (kv *ShardCtrler) check_dup(op Op) (bool, OpResult) {
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

func (sc *ShardCtrler) applyRes2Chan(reqId int64, result OpResult) {
	sc.mu.Lock()
	reqChan, ok := sc.Req2Result[reqId]
	sc.mu.Unlock()
	if ok {
		reqChan <- result
	}
}

func (sc *ShardCtrler) update(clerk int64, latestSeq int64, latestRes OpResult) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.clerkLatestSeq[clerk] = latestSeq
	sc.clerkLatestRes[clerk] = latestRes
}

func (sc *ShardCtrler) run() {
	for perCommand := range sc.applyCh {
		if perCommand.CommandValid {
			op := perCommand.Command.(Op)
			if need_process, res := sc.check_dup(op); !need_process {
				sc.applyRes2Chan(op.ReqId, res)
				continue
			}
			ret := OpResult{}
			switch op.Type {
			case QUERY:
				// If the number is -1 or bigger than the biggest known configuration number,
				//the shardctrler should reply with the latest configuration.
				var targetConfig int
				targetConfig = op.Num
				if op.Num == -1 || op.Num >= len(sc.configs) {
					targetConfig = len(sc.configs) - 1
					NoTimeLogInfo("%d check------------------\n", sc.me)
					for i, g := range sc.configs[targetConfig].Shards {
						NoTimeLogInfo("{%d:%d}, ", i, g)
					}
					NoTimeLogInfo("\n------------------\n")
				}
				ret.Config = sc.configs[targetConfig]
				ret.Error = OK
			case JOIN:
				newConfig := CopyConfig(&sc.configs[len(sc.configs)-1])
				for k, v := range op.Servers {
					newConfig.Groups[k] = v
				}
				newConfig.LoadBalance()
				sc.configs = append(sc.configs, newConfig)
				ret.Error = OK
			case LEAVE:
				newConfig := CopyConfig(&sc.configs[len(sc.configs)-1])
				for _, deleteGid := range op.GIDs {
					delete(newConfig.Groups, deleteGid)
				}
				newConfig.LoadBalance()
				sc.configs = append(sc.configs, newConfig)
				ret.Error = OK
			case MOVE:
				newConfig := CopyConfig(&sc.configs[len(sc.configs)-1])
				newConfig.Shards[op.Shard] = op.GIDs[0]
				sc.configs = append(sc.configs, newConfig)
				ret.Error = OK
			}
			sc.update(op.ClerkId, op.CmdSeq, ret)
			sc.applyRes2Chan(op.ReqId, ret)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.Req2Result = make(map[int64]chan OpResult)
	sc.clerkLatestRes = make(map[int64]OpResult)
	sc.clerkLatestSeq = make(map[int64]int64)

	go sc.run()
	return sc
}
