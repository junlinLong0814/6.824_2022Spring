package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	//https://www.zhihu.com/column/c_1533964917711794176

	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func CopyConfig(bak *Config) Config {
	newConfig := Config{}
	newConfig.Num = bak.Num + 1
	for i := 0; i < len(bak.Shards); i++ {
		newConfig.Shards[i] = bak.Shards[i]
	}
	newConfig.Groups = make(map[int][]string)
	for k, v := range bak.Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
}

//TODO: To be optimized
func (cfg *Config) LoadBalance() {
	if len(cfg.Groups) == 0 {
		//no clusters
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	group2Count := make(map[int]int)
	group2Shards := make(map[int][]int)
	gids := make([]int, 0)
	freeShards := make([]int, 0)

	for key := range cfg.Groups {
		group2Count[key] = 0
		group2Shards[key] = make([]int, 0)
		gids = append(gids, key)
	}
	sort.Ints(gids)
	//remove old gids
	for i := 0; i < NShards; i++ {
		gid := cfg.Shards[i]
		if _, ok := cfg.Groups[gid]; !ok {
			cfg.Shards[i] = 0
			freeShards = append(freeShards, i)
		} else {
			group2Count[gid]++
			if _, ok := group2Shards[gid]; ok {
				group2Shards[gid] = append(group2Shards[gid], i)
			}
		}
	}

	// for _, free := range freeShards {
	// 	NoTimeLogInfo("%d,", free)
	// }
	// NoTimeLogInfo("\n")

	for i := 0; i < len(freeShards); i++ {
		_, minGroup := getMinCount(&gids, &group2Count)
		cfg.Shards[freeShards[i]] = minGroup
		group2Count[minGroup]++
		if arr, ok := group2Shards[minGroup]; ok {
			arr = append(arr, freeShards[i])
			group2Shards[minGroup] = arr
		}
	}
	var maxCount, maxGroup int
	var minCount, minGroup int
	maxCount, maxGroup = getMaxCount(&gids, &group2Count)
	minCount, minGroup = getMinCount(&gids, &group2Count)

	for !(minCount == maxCount || minCount == maxCount-1) {
		group2Count[minGroup]++
		group2Count[maxGroup]--

		shards, oriLen := group2Shards[maxGroup], len(group2Shards[maxGroup])
		lastShard := shards[oriLen-1]
		group2Shards[maxGroup] = shards[0 : oriLen-1]

		cfg.Shards[lastShard] = minGroup
		if arr, ok := group2Shards[minGroup]; ok {
			arr = append(arr, lastShard)
			group2Shards[minGroup] = arr
		}

		maxCount, maxGroup = getMaxCount(&gids, &group2Count)
		minCount, minGroup = getMinCount(&gids, &group2Count)
	}

	// for i, j := range cfg.Shards {
	// 	NoTimeLogInfo("[%d->%d]\n", i, j)
	// }
}

func getMaxCount(gids *[]int, count *map[int]int) (int, int) {
	mgid, mnum := 0, 0
	for _, gid := range *gids {
		num := (*count)[gid]
		if num > mnum {
			mgid, mnum = gid, num
		}
	}
	return mnum, mgid
}

func getMinCount(gids *[]int, count *map[int]int) (int, int) {
	mgid, mnum := 0, NShards+1
	for _, gid := range *gids {
		num := (*count)[gid]
		if num < mnum {
			mgid, mnum = gid, num
		}
	}
	return mnum, mgid
}

const (
	OK      = "OK"
	TIMEOUT = "TIMEOUT"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	ClerkId int64
	Cmdseq  int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs    []int
	ClerkId int64
	Cmdseq  int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard   int
	GID     int
	ClerkId int64
	Cmdseq  int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num     int // desired config number
	ClerkId int64
	Cmdseq  int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
