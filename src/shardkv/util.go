package shardkv

import (
	"bytes"
	"fmt"
	"time"

	"6.824/labgob"
	"6.824/shardctrler"
)

func (kv *ShardKV) serilizeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.currentCfg)
	e.Encode(kv.prevCfg)
	e.Encode(kv.shards)
	kv.mu.Unlock()
	return w.Bytes()
}

func (kv *ShardKV) deserilizeState(snapShot []byte) {
	if snapShot == nil || len(snapShot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)

	var curConfig shardctrler.Config
	var prevConfig shardctrler.Config
	var shards [shardctrler.NShards]shard

	if d.Decode(&curConfig) != nil ||
		d.Decode(&prevConfig) != nil ||
		d.Decode(&shards) != nil {
		//sth wrong
	} else {
		kv.mu.Lock()
		kv.currentCfg = curConfig
		kv.prevCfg = prevConfig
		kv.shards = shards
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) applyRes2Chan(shardid int, reqId int64, result OpResult) {
	kv.mu.Lock()
	//check the shard status
	reqChan, ok := kv.Req2Result[reqId]
	kv.mu.Unlock()
	if ok {
		reqChan <- result
	}
}

func (kv *ShardKV) check_dup(shardid int, op Op) (bool, OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//check the shard status
	clerkLatestSeqs, clerkLatestRess := kv.shards[shardid].ClerkLatestSeq, kv.shards[shardid].ClerkLatestRes

	if client_last_process_seq, ok := clerkLatestSeqs[op.ClerkId]; ok {
		if op.CmdSeq <= client_last_process_seq {
			// need not to process
			return false, clerkLatestRess[op.ClerkId]
		}
	}
	// need to process
	return true, OpResult{}
}

// Debugging
const Log = 0

func TimeInfo() string {
	return "[" + time.Now().Format("2006-01-02 15:04:05.000") + "]"
}

func NoTimeLogInfo(format string, a ...interface{}) (n int, err error) {
	if Log > 0 {
		fmt.Printf(format, a...)
	}
	return
}

func LogInfo(format string, a ...interface{}) (n int, err error) {
	if Log > 0 {
		format = TimeInfo() + format
		fmt.Printf(format, a...)
	}
	return
}
