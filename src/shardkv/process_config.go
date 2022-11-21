package shardkv

import "time"

func (kv *ShardKV) pullLatestConfig() {
	//every 100ms query
	ticker := time.NewTicker(time.Millisecond * 100)
	for range ticker.C {
		kv.mu.Lock()
		//check the status
		//only the leader can query the config
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.mu.Unlock()
			continue
		}

		if !kv.canPullConfig() {
			//the previous reconfiguration has not been completed
			kv.mu.Unlock()
			continue
		}

		queryNum := kv.currentCfg.Num + 1
		kv.mu.Unlock()

		queryConfig := kv.clk.Query(queryNum)

		kv.mu.Lock()
		able2update := queryConfig.Num == kv.currentCfg.Num+1
		kv.mu.Unlock()
		//LogInfo("{%d}:  Able2Update:%t\n", kv.me, able2update)
		//compare the query config and my config
		if able2update {
			op := Op{
				Type:      UPDATE_CONFIG,
				NewConfig: queryConfig,
			}
			//notify all members to update config by raft
			kv.rf.Start(op)
		}
	}
	ticker.Stop()
}

func (kv *ShardKV) updateConfig(op Op) {
	//can't across config to update
	kv.mu.Lock()
	if kv.currentCfg.Num+1 == op.NewConfig.Num {

		kv.prevCfg = kv.currentCfg
		kv.currentCfg = op.NewConfig
		//check the status of all shards and reset the status which are change
		kv.checkAndreset()
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) canPullConfig() bool {
	for _, shard := range kv.shards {
		if shard.State == Pulling || shard.State == BePulling {
			return false
		}
	}
	return true
}
