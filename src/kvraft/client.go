package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cmdSeq int64
	me     int64
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	ck.cmdSeq = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	tryLeader := ck.leader
	//curSeq := nrand() //per command must use same seq!3A bug!
	ck.cmdSeq++
	curSeq := ck.cmdSeq
	for {
		args := GetArgs{
			Key:    key,
			Clerk:  ck.me,
			CmdSeq: curSeq,
		}
		reply := GetReply{}
		replyChan := make(chan bool, 1)
		go ck.sendGet(tryLeader, replyChan, &args, &reply)
		//LogInfo("***[%d] send 2 [%d] Get{Seq:[%d]}***\n", ck.me, tryLeader, args.CmdSeq)
		select {
		case ok := <-replyChan:
			if ok {
				if reply.Err == OK {
					ck.leader = tryLeader
					return reply.Value
				} else if reply.Err == ErrNoKey {
					ck.leader = tryLeader
					return ""
				} else {
					tryLeader = (tryLeader + 1) % len(ck.servers)
				}
			} else {
				//network fault
				tryLeader = (tryLeader + 1) % len(ck.servers)
			}

		case <-time.After(TIME_OUT * time.Millisecond):
			//time out
			//maybe leader crash,try new leader
			tryLeader = (tryLeader + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	tryLeader := ck.leader
	//curSeq := nrand() //per command must use same seq!3A bug!
	ck.cmdSeq++
	curSeq := ck.cmdSeq
	for {
		args := PutAppendArgs{
			Key:    key,
			Clerk:  ck.me,
			CmdSeq: curSeq,
			Op:     op,
			Value:  value,
		}
		reply := PutAppendReply{}
		replyChan := make(chan bool, 1)
		go ck.sendPutAppend(tryLeader, replyChan, &args, &reply)
		select {
		case ok := <-replyChan:
			if ok {
				if reply.Err == OK {
					ck.leader = tryLeader
					return
				}
			}
			//wrong leader || server's timeout || network fault
			tryLeader = (tryLeader + 1) % len(ck.servers)
		case <-time.After(TIME_OUT * time.Millisecond):
			//time out
			//maybe leader crash,try new leader
			tryLeader = (tryLeader + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendGet(tryLeader int, ch chan bool, args *GetArgs, reply *GetReply) {
	//LogInfo("***[%d] send reply to [%d] Get{Seq:[%d],key:[%s]}\n", ck.me, tryLeader, args.CmdSeq, args.Key)
	ok := ck.servers[tryLeader].Call("KVServer.Get", args, reply)
	//LogInfo("***[%d] got reply from [%d] Get{Seq:[%d],key:[%s]} Err [%s]***\n", ck.me, tryLeader, args.CmdSeq, args.Key, reply.Err)
	ch <- ok
}

func (ck *Clerk) sendPutAppend(tryLeader int, ch chan bool, args *PutAppendArgs, reply *PutAppendReply) {
	//LogInfo("***[%d] send reply to [%d] %s{Seq:[%d],key:[%s]}\n", ck.me, tryLeader, args.Op, args.CmdSeq, args.Key)
	ok := ck.servers[tryLeader].Call("KVServer.PutAppend", args, reply)
	//LogInfo("***[%d] got reply from [%d] %s{Seq:[%d],key:[%s]} Err [%s]***\n", ck.me, tryLeader, args.Op, args.CmdSeq, args.Key, reply.Err)
	ch <- ok
}
