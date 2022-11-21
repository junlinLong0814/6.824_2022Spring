package shardkv

import (
	"time"

	"6.824/shardctrler"
)

type shardStatus uint8

const (
	/*normal*/
	Invalid shardStatus = iota
	Working
	/*my group is respondsible for this shard,
	  but this shard doesn't provide service until data is pulled from the previous group respondsible for this shard*/
	Pulling
	/*my group isn't respondsible for this shard and this shard can't provide service,
	  but need to send the shard's data to the group respondsible for this shard in current config*/
	BePulling //tips: if don't complete the 'garbage collection' test, this status is euqal to Invalid
	/*GC state,when new group is finishing pulling data, BePulling -> GCable*/
	GCable
)

type shard struct {
	State shardStatus
	Data  map[string]string /*all kv pairs in this shard*/
	//just save the latest result
	ClerkLatestSeq map[int64]int64
	ClerkLatestRes map[int64]OpResult
}

func (sd *shard) Copy() shard {
	res := shard{
		Data:           make(map[string]string),
		State:          sd.State,
		ClerkLatestSeq: make(map[int64]int64),
		ClerkLatestRes: make(map[int64]OpResult),
	}

	for k, v := range sd.ClerkLatestRes {
		res.ClerkLatestRes[k] = v
	}

	for k, v := range sd.ClerkLatestSeq {
		res.ClerkLatestSeq[k] = v
	}

	for k, v := range sd.Data {
		res.Data[k] = v
	}

	return res
}
func (kv *ShardKV) checkAndreset() {
	//check the changed shards
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.currentCfg.Shards[i] != kv.prevCfg.Shards[i] {
			oldGroup, newGroup := kv.prevCfg.Shards[i], kv.currentCfg.Shards[i]
			if oldGroup == kv.gid && newGroup != kv.gid {
				//shard leave
				if _, ok := kv.rf.GetState(); ok {
					LogInfo("{%d,prevCfg:%d->curCfg:%d}: [%d]shard Working->BePulling!\n", kv.gid, kv.prevCfg.Num, kv.currentCfg.Num, i)
				}

				kv.shards[i].State = BePulling
			} else if oldGroup != kv.gid && newGroup == kv.gid {
				//shard join
				kv.shards[i].ClerkLatestRes = make(map[int64]OpResult)
				kv.shards[i].ClerkLatestSeq = make(map[int64]int64)
				kv.shards[i].Data = make(map[string]string)
				if oldGroup == 0 {
					if _, ok := kv.rf.GetState(); ok {
						LogInfo("{%d,prevCfg:%d->curCfg:%d}:  [%d] Invalid->Working!\n", kv.gid, kv.prevCfg.Num, kv.currentCfg.Num, i)
					}
					kv.shards[i].State = Working
				} else {
					if _, ok := kv.rf.GetState(); ok {
						LogInfo("{%d,prevCfg:%d->curCfg:%d}:  [%d] Workng->Pulling!\n", kv.gid, kv.prevCfg.Num, kv.currentCfg.Num, i)
					}
					kv.shards[i].State = Pulling
				}
			}
		}
	}
}

func (kv *ShardKV) pushShards() {
	ticker := time.NewTicker(time.Millisecond * 100)
	for range ticker.C {
		kv.mu.Lock()

		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}
		//check all the shards in my group
		//and pull new shards's data from old group
		for i, curShard := range kv.shards {
			if curShard.State == BePulling {
				curGroup := kv.currentCfg.Shards[i]
				curServer, curConfigNum := kv.currentCfg.Groups[curGroup], kv.currentCfg.Num
				//LogInfo("gid:%d push [%d]shard 2 [%d]group\n", kv.gid, i, curGroup)
				go kv.sendPushShard(i, curServer, curConfigNum)
			}
		}
		kv.mu.Unlock()
	}
	ticker.Stop()
}

func (kv *ShardKV) sendPushShard(shardid int, curServers []string, curConfigNum int) {
	for {
		for _, server := range curServers {
			clientEnd := kv.make_end(server)

			args := PushShardArgs{
				ConfigNum: curConfigNum,
				ShardId:   shardid,
				ShardData: kv.shards[shardid].Copy(),
			}
			reply := PushShardReply{}
			//LogInfo("gid:%d push [%d]shard\n", kv.gid, shardid)
			ok := clientEnd.Call("ShardKV.GetShards", &args, &reply)
			if ok {
				if reply.Err != OK {
					continue
				}
				kv.mu.Lock()
				valid := curConfigNum == kv.currentCfg.Num && kv.shards[shardid].State == BePulling
				LogInfo("gid:%d, curConfigNum:%d, kv.currentCfg.Num:%d, kv.shards[%d].State:%d\n",
					kv.gid, curConfigNum, kv.currentCfg.Num, shardid, kv.shards[shardid].State)
				kv.mu.Unlock()

				//if push shard done, set status to GCable
				if valid {
					op := Op{
						Type:           CHANGE_SHARD,
						ShardId:        shardid,
						NewStatus:      GCable,
						ShardConfigNum: curConfigNum,
					}
					kv.rf.Start(op)
				}
				return
			} else {
				//LogInfo("[wrongStatus] from gid[%d] ok[%t] getshards[%d] err[%s]\n", kv.prevCfg.Shards[shardid], ok, shardid, reply.Err)
				continue
			}
		}
	}

}

func (kv *ShardKV) GetShards(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isleader := kv.rf.GetState(); !isleader {
		//only leader can send shards
		//because after that leaders can change BePulling->GCable
		reply.ConfigNum = -1
		reply.Err = ErrWrongLeader
		return
	}
	//check the config num and target shard's state
	if args.ConfigNum < kv.currentCfg.Num {
		//example:
		//A、B group,in confg[0], A has{0}shard, B has{1,2}shard
		//in config[1],A->{0,1},B->{2}shard
		//and B push{1}shard data to A
		//A receive {1}shard and reply the OK to B then all shards in A change to working,so A is in config[1] now
		//But the reply lost, so B can't update config[0]->config[1] because B has BePulling shard,so it will send push to A
		//Now A's config num > B's config Num, so A shoule send OK to B
		//it's ok to send ok to peer when my config newer than others
		reply.ConfigNum = args.ConfigNum
		reply.Err = OK
	} else if args.ConfigNum == kv.currentCfg.Num && kv.shards[args.ShardId].State == Pulling {
		reply.ConfigNum = args.ConfigNum
		reply.Err = OK

		op := Op{
			Type:           CHANGE_SHARD,
			ShardId:        args.ShardId,
			NewStatus:      Working,
			ShardConfigNum: args.ConfigNum,
			NewShard:       args.ShardData,
		}
		kv.rf.Start(op)
	} else {
		//sth wrong
		//TODO
		reply.ConfigNum = -1
		reply.Err = ErrWrongLeader
	}

}

func (kv *ShardKV) changeShardState(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//Pulling -> Working
	//BePulling-> GCable (recover garbage)-> Working

	if op.ShardConfigNum != kv.currentCfg.Num {
		return
	}
	if op.NewStatus == Working {
		if kv.shards[op.ShardId].State == Pulling {
			kv.shards[op.ShardId] = op.NewShard.Copy()
			kv.shards[op.ShardId].State = Working
		}
	} else if op.NewStatus == GCable {
		if kv.shards[op.ShardId].State == BePulling {
			kv.shards[op.ShardId].ClerkLatestRes = nil
			kv.shards[op.ShardId].ClerkLatestSeq = nil
			kv.shards[op.ShardId].Data = nil
			kv.shards[op.ShardId].State = Working
		}
	}
}
