package shardkv

import "time"

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//check i am the target group
	shardId, clerk := key2shard(args.Key), args.Clerk
	kv.mu.Lock()
	targetGroup, statue := kv.currentCfg.Shards[shardId], kv.shards[shardId].State
	if targetGroup != kv.gid || statue != Working {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	if latestSeq, ok := kv.shards[shardId].ClerkLatestSeq[clerk]; ok {
		if latestSeq == args.CmdSeq {
			reply.Err = kv.shards[shardId].ClerkLatestRes[clerk].Error
			reply.Value = kv.shards[shardId].ClerkLatestRes[clerk].Result
			kv.mu.Unlock()
			return
		} else if latestSeq > args.CmdSeq {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	command := Op{
		Type:    GET,
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
		select {
		case opResult := <-clerkResChan:
			reply.Err = opResult.Error
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//check i am the target group && shard status
	shardId, clerk := key2shard(args.Key), args.Clerk
	kv.mu.Lock()
	targetGroup, statue := kv.currentCfg.Shards[shardId], kv.shards[shardId].State
	if targetGroup != kv.gid || statue != Working {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	if latestSeq, ok := kv.shards[shardId].ClerkLatestSeq[clerk]; ok {
		if latestSeq == args.CmdSeq {
			reply.Err = kv.shards[shardId].ClerkLatestRes[clerk].Error
			kv.mu.Unlock()
			return
		} else if latestSeq > args.CmdSeq {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	command := Op{
		Type:    args.Op,
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
		select {
		case opResult := <-clerkResChan:
			reply.Err = opResult.Error
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

func (kv *ShardKV) processClient(shardid int, op Op) OpResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//check the shard status
	result := OpResult{
		Error:  OK,
		Result: "",
	}
	if status := kv.shards[shardid].State; status == Working {
		shardkv_map := kv.shards[shardid].Data
		switch op.Type {
		case GET:
			if val, ok := shardkv_map[op.Key]; ok {
				result.Result = val
			} else {
				result.Error = ErrNoKey
			}
		case PUT:
			shardkv_map[op.Key] = op.Value
		case APPEND:
			if val, ok := shardkv_map[op.Key]; ok {
				shardkv_map[op.Key] = val + op.Value
			} else {
				shardkv_map[op.Key] = op.Value
			}
		}
	} else if status == Pulling {
		result.Error = ErrPulling
	} else {
		result.Error = ErrWrongGroup
	}
	return result
}

func (kv *ShardKV) update(shardid int, op Op, result OpResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if status := kv.shards[shardid].State; status == Working {
		kv.shards[shardid].ClerkLatestRes[op.ClerkId] = result
		kv.shards[shardid].ClerkLatestSeq[op.ClerkId] = op.CmdSeq
	}
}
