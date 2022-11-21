package shardkv

import (
	"sync"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	PUT           = "Put"
	APPEND        = "Append"
	GET           = "Get"
	UPDATE_CONFIG = "UpdateConfig"
	CHANGE_SHARD  = "ChangeShard"
	TIME_OUT      = 100
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	//for group member process client get/put/append command
	Type    string
	Key     string
	Value   string
	ReqId   int64
	ClerkId int64
	CmdSeq  int64

	//for group member sync latest configs
	NewConfig shardctrler.Config
	//for group member change shards status in current config
	NewShard       shard
	NewStatus      shardStatus
	ShardId        int
	ShardConfigNum int
}

type OpResult struct {
	Error  Err
	Result string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	//need to save in snapshot
	currentCfg shardctrler.Config //current config in my group,need to
	prevCfg    shardctrler.Config //prev config(current config.num - 1) in my group
	shards     [shardctrler.NShards]shard

	//don't need to save in snapshot
	Req2Result map[int64]chan OpResult //for notifying coroutines which processing client command
	clk        *shardctrler.Clerk      //for querying the latest config
	persister  *raft.Persister

	/*To Think :
	 *per shardkv need to save all old config? at least,we shoule save all shards in BePulling state from historical config version ?
	 *Example: GroupA and GroupB are in (Config.Num == 2),
	 *then GroupA doesn't persist and crash, it's CurrentConfigNum reset to 0
	 *A will send Query(1) to the master and master will tell A config[1]'s situation
	 *So A will let B send config[1]'s shards, if B doesn't have, A will block!
	 */
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) run() {
	for perCommand := range kv.applyCh {
		if perCommand.CommandValid {
			op := perCommand.Command.(Op)
			targetShard := key2shard(op.Key)
			//process get/put/append commands
			if op.Type == PUT || op.Type == APPEND || op.Type == GET {
				//check is duplicate or not
				need_process, res := kv.check_dup(targetShard, op)
				if !need_process {
					kv.applyRes2Chan(targetShard, op.ReqId, res)
				} else {
					//new client command,process it
					ret := kv.processClient(targetShard, op)
					kv.update(targetShard, op, ret)
					kv.applyRes2Chan(targetShard, op.ReqId, ret)
				}
			} else if op.Type == UPDATE_CONFIG {
				kv.updateConfig(op)
			} else if op.Type == CHANGE_SHARD {
				kv.changeShardState(op)
			}
			if kv.maxraftstate != -1 && kv.maxraftstate <= kv.persister.RaftStateSize() {
				//need to save sanpshot
				curSanpShot := kv.serilizeState()
				go kv.rf.Snapshot(perCommand.CommandIndex, curSanpShot)
			}
		} else if perCommand.SnapshotValid {
			kv.deserilizeState(perCommand.Snapshot)
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.Req2Result = make(map[int64]chan OpResult)

	kv.clk = shardctrler.MakeClerk(kv.ctrlers)
	kv.persister = persister

	kv.currentCfg = shardctrler.Config{}
	kv.prevCfg = shardctrler.Config{}

	kv.deserilizeState(persister.ReadSnapshot())

	go kv.pullLatestConfig()
	go kv.run()
	go kv.pushShards()

	return kv
}
