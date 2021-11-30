package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"labgob"
	"labrpc"
	"bytes"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type state string
const(
	follower=state("Follower")
	leader=state("Leader")
	candidate=state("Candidate")
)
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	context.Context
}

type Log struct {
	Command interface{}
	Term int
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	state state
	currentTerm int
	voteFor int
	log []Log
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	voteNum int
	//heartBeat []chan bool
	//stateChange chan state
	//agreeVote []chan bool
	stopHeartBeat chan bool
	changeFollower chan bool
	changeLeader chan bool
	// stopAppendEntry chan bool
	//candidateChan chan bool
	//stateChange chan state
	applyCh chan ApplyMsg
	//electionTimer *time.Timer
	timer *time.Timer
	//时间片用完没接收会怎么样，
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) GetLastLogIno() (int,int) {
	lastId := len(rf.log)-1
	lastTerm := rf.log[lastId].Term
	return lastId,lastTerm
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state==leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	//rf.readPersist(rf.persister.ReadRaftState())
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor)!=nil || d.Decode(&log) != nil {
		DPrintf("Decode error")
	}else {
		rf.currentTerm = currentTerm
		rf.voteFor = votedFor
		rf.log = log
	}
	//msg := ApplyMsg{UseSnapshot: true, Snapshot: data}
	//go func() {
	//	rf.chanApply <- msg
	//}()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

type AppendEntryReply struct {
	// Your data here (2A).
	Term int
	LogIndex int
	Success bool
	ConflictTerm int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	/*if one server’s current term is smaller than the other’s,
	 then it updates its current term to the larger value.
	If a candidate or leader discovers that its term is out of date,
	it immediately reverts to follower state*/

	/*If the candidate’s log is at least as up-to-date as any other log
	in that majority (where “up-to-date” is defined precisely
	below), then it will hold all the committed entries*/

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted=false
	//Each server will vote for at most one candidate in a given term,
	DPrintf("Func RequestVote %s:%d currentTerm=%d  candidateID=%d candidateTerm=%d",
		rf.state,rf.me,rf.currentTerm,args.CandidateId,args.Term)

	// Func RequestVote Leader:1 currentTerm=4  candidateID=0 candidateTerm=5 莫名其妙消失.......
	// appenEntry没来得及运行。。。。。。。。channel阻塞
	// Func RequestVote Candidate:1 currentTerm=13  candidateID=0 candidateTerm=14


	// 由于当选的leader需要最新的日志，所以即使当前节点已经投过票也需要再次比较follower是否有最新的日志
	// rf.voteFor != args.CandidateId 考虑RPC返回失效
	if args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.Term = rf.currentTerm
		return
	}

	//If the candidate’s current term is smaller than the term,
	if rf.currentTerm > args.Term {
		reply.Term=rf.currentTerm
		return
	}
	//If the term is smaller than the candidate’s current term,

	if args.Term > rf.currentTerm {
		//rf.currentTerm= args.Term
		//1个chan时可能同时发生超时、状态变换导致管道阻塞 当时错误地理解了channel。。。。

		/*
			向chanel发送数据，但是state已经是follower。。。时间片不断被重置，不在运行该代码块，
			一直阻塞，无法接受数据
		*/
		rf.state = follower
		rf.voteFor = -1
		rf.voteNum = 0
		rf.currentTerm = args.Term
		//rf.ResetHeartBeatTimer()
		rf.persist()
		/*if rf.state == candidate {
			rf.stateChange <- follower
		} else if rf.state == leader {
			//rf.stopAppendEntry <- true
			rf.stopHeartBeat <- true
		}
		rf.ChangeState(follower)
		rf.ResetHeartBeatTimer()*/

	}

	//leader is log must contains all committed entries
	//Raft determines which of two logs is more up-to-date by comparing
	//the index and term of the last entries in the logs.
	reply.Term = rf.currentTerm
	//
	lastLogId,_ := rf.GetLastLogIno()
	lastLog := rf.log[lastLogId]
	/*DPrintf("Func RequestVote %s:%d lastLog.Term=%d args.LastLogTerm=%d ",
	rf.state,rf.me,lastLog.Term,args.LastLogTerm)*/
	if lastLog.Term > args.LastLogTerm {
		return
	}
	//DPrintf("Func RequestVote %s:%d lastLog.Term=%d candidateID=%d  args.LastLogTerm=%d",
	//rf.state,rf.me,lastLog.Term,args.CandidateId,args.LastLogTerm)
	if lastLog.Term < args.LastLogTerm {
		//rf.agreeVote <- true

		/*if rf.state == candidate {
			rf.candidateChan <- false
		} else if rf.state == leader {
			//rf.stopAppendEntry <- true
			rf.stopHeartBeat<- true
		}
		rf.ChangeState(follower)*/
		/*if rf.state != follower {
			rf.state = follower
			rf.voteFor = -1
			rf.voteNum = 0
		}*/

		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		//rf.ResetHeartBeatTimer()
		rf.ResetElectionTimer()
		//DPrintf("Func RequestVote %s:%d  currentTerm=%d agreeVote to candidateID=%d candidateTerm=%d",
		//rf.state,rf.me,rf.currentTerm,args.CandidateId,args.Term)
		return
	}
	if lastLogId  > args.LastLogIndex {
		return
	}


	/*if rf.state == candidate {
		rf.candidateChan <- false
	} else if rf.state == leader {
		//rf.stopAppendEntry <- true
		rf.stopHeartBeat <- true
	}
	rf.ChangeState(follower)*/

	/*if rf.state != follower {
		rf.state = follower
		rf.voteFor = -1
		rf.voteNum = 0
	}*/
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	rf.persist()
	rf.ResetElectionTimer()
	//rf.ResetHeartBeatTimer()
	/*DPrintf("Func RequestVote %s:%d  currentTerm=%d agreeVote to candidateID=%d candidateTerm=%d",
	rf.state,rf.me,rf.currentTerm,args.CandidateId,args.Term)*/
	return
}
func (rf *Raft) HeartBeat(args *AppendEntryArgs,reply *AppendEntryReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	/*DPrintf("Func HeartBeat %s:%d currentTem=%d receive HeartBeat from Leader:%d currentTerm=%d!!!!!!",
	rf.state,rf.me,rf.currentTerm,args.LeaderId,args.Term)*/
	/*
		heartbeat 时为什么不需要比较最后一条日志的索引和log term？？
	*/
	if rf.currentTerm > args.Term  {
		reply.Term = rf.currentTerm
		reply.Success = false
	}else {
		if rf.state == candidate || rf.state == leader {
			rf.state = follower
		}
		if rf.currentTerm < args.Term {
			rf.voteFor = -1
			rf.voteNum = 0
			rf.currentTerm = args.Term
			rf.persist()
		}
		reply.Term = rf.currentTerm
		rf.ResetHeartBeatTimer()
	}
}
func (rf *Raft) AppendEntry(args *AppendEntryArgs,reply *AppendEntryReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Func AppendEntry %s:%d currentTem=%d receive HeartBeat from Leader:%d currentTerm=%d!!!!!!",
		rf.state,rf.me,rf.currentTerm,args.LeaderId,args.Term)
	/*
		heartbeat 时为什么不需要比较最后一条日志的索引和log term？？
	*/
	if rf.currentTerm > args.Term  {
		reply.Term = rf.currentTerm
		reply.Success = false
	}else {
		if rf.state == candidate || rf.state == leader {
			rf.state = follower
		}
		if rf.currentTerm < args.Term {
			rf.voteFor = -1
			rf.voteNum = 0
			rf.currentTerm = args.Term
			rf.persist()
		}
		reply.Term = rf.currentTerm
		defer rf.ResetHeartBeatTimer()


		/*
			日志同步时不能随意截断日志，因为可能存在过时的RPC
			不同leader的RPC：
				1. old同步完，new同步
					根据old leader的commitIndex提交没问题
					这里截断没问题
				2. new同步完，old RPC到达
					因为term比old的大,直接拒绝
			相同leader的RPC：
				都是一样的日志，仅是长短不同，这时不能截断。
				old RPC: len(log)
				此时follower len > len(log),不能截断。
		*/
		//DPrintf("LogIndex:%d Leader:%d is LogTerm:%d me:%d\n",
		//	args.PrevLogIndex,args.LeaderId, args.PrevLogTerm,rf.me)
		/*
				args.PrevLogIndex   leader想要同步的日志索引
			  	reply.LogIndex 		follower想要同步的日志索引
		*/
		//当前follower还没有这条日志
		lastId,_ := rf.GetLastLogIno()
		DPrintf("Func AppendEntry: Follower:%d agrs.PreVLogIndex=%d FOLLOWERLastlogId=%d*******",
			rf.me,args.PrevLogIndex,lastId)
		if args.PrevLogIndex > lastId {
			//DPrintf("**************************\n")
			reply.LogIndex = lastId
			reply.Success = false
			reply.ConflictTerm = -1
			return
		}

		//follower有这条日志但是term不同,删除
		//过最后一个test需要优化：找到当前term的第一个日志发送给leader
		//DPrintf("Func AppendEntry: Follower:%d Leader:%d leader LogIndex:%d  leader LogTerm:%d &&&& follower %d is LogTerm is %d****",
		//rf.me,args.LeaderId,args.PrevLogIndex, args.PrevLogTerm,rf.me,rf.log[args.PrevLogIndex].Term)
		// args.PrevLogTerm == -2
		if args.PrevLogTerm == -1  {
			reply.Success = false
			reply.ConflictTerm = -1
			reply.LogIndex = args.PrevLogIndex-1
		} else {
			//	DPrintf("Func AppendEntry: args.PrevLogTerm=%d rf.log[%d].Term=%d LOGlen=%d",args.PrevLogTerm,args.PrevLogIndex,
			//	rf.log[args.PrevLogIndex].Term,len(args.Entries))
			if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
				//DPrintf("((((((((((()))))))))))")
				term := rf.log[args.PrevLogIndex].Term
				reply.Success = false
				reply.ConflictTerm = term
				reply.LogIndex = args.PrevLogIndex-1
				rf.log = rf.log[:args.PrevLogIndex]
				rf.persist()
				for i := args.PrevLogIndex-1; i >= 0; i-- {
					if i == 0 || rf.log[i].Term != term  {
						reply.LogIndex = i + 1
						break
					}
				}
				if reply.LogIndex <= 0 {
					DPrintf("Stage 2 appear replyLogIndex <0\n")
				}
				return
			}
			//DPrintf("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
			//优化
			lastId, _ = rf.GetLastLogIno()
			reply.LogIndex = args.PrevLogIndex + 1
			reply.Success = true
			for i := args.PrevLogIndex; i < len(args.Entries); i++ {
				if i > lastId {
					rf.log = append(rf.log, args.Entries[i:]...)
					rf.persist()
					reply.LogIndex = len(rf.log)
					break
				}
				if rf.log[i].Term != args.Entries[i].Term {
					rf.log = rf.log[:i]
					rf.log = append(rf.log, args.Entries[i:]...)
					/*
						for j:=i; j<len(args.Entries); j++ {
							rf.log = append(rf.log,args.Entries[j])
						}
					*/
					rf.persist()
					reply.LogIndex = len(rf.log)
					break
				}
				reply.LogIndex = i + 1
			}
			//DPrintf("FOLLOWER TERM = LEADER TERM,Leaded Log len=%d ,AppendSuccess FOLLOWER:%d replyIndex:%d",
			//	len(args.Entries),rf.me,reply.LogIndex)
			if reply.LogIndex <= 0 {
				DPrintf("Stage 3 appear replyLogIndex <0\n")
			}
			// 过时的RPC一定要考虑
			//注意prevLogIndex可能大于leadercommit,leadercommit不是最新
			//minId := judge(&args.PrevLogIndex,&args.LeaderCommit)
			judge :=func(a *int,b *int) int {
				if *a > *b {
					return *b
				}
				return *a
			}
			if args.LeaderCommit > rf.commitIndex  {
				lastId,_ := rf.GetLastLogIno()
				rf.commitIndex = judge(&lastId,&args.LeaderCommit)
			}
			//DPrintf("&&&&FOLLOWER:%d rf.lastApplied=%d rf.commitIndex=%d&&&&&&",rf.me,rf.lastApplied,
			//rf.commitIndex)
			if rf.lastApplied <= rf.commitIndex {
				go rf.ApplyStateMachine(&rf.lastApplied,&rf.commitIndex,rf.log)
			}
		}
	}
}


func (rf *Raft) UpdateCommitIndex (log []Log) {

	/*
		大多数的日志已经一致,更新commitedIndex
		leader只允许commit当前term的entry，其实是指积压着之前已经被majority认可的entry，
		直到当前term也被majority认可，然后统一commit。
	*/
	rf.mu.Lock()
	if rf.state == leader {
		//DPrintf("*******************************************%d",len(args.Entries))
		//DPrintf(string(rf.state)+" "+strconv.Itoa(rf.me)+" currentTerm:"+strconv.Itoa(rf.currentTerm)+"CommitInedx:"+strconv.Itoa(rf.commitIndex))
		for i := len(log)-1; i > rf.commitIndex; i-- {
			//DPrintf("LogId: "+strconv.Itoa(i+1)+" LogTerm:"+strconv.Itoa(args.Entries[i].Term))
			if log[i].Term == rf.currentTerm {
				num := 1
				for j := range rf.peers {
					if j != rf.me {
						//	DPrintf(strconv.Itoa(j)+":"+strconv.Itoa(rf.matchIndex[j]))
						if rf.matchIndex[j] >= i{
							num++
						}
					}
				}
				//DPrintf("num="+strconv.Itoa(num)+" len="+strconv.Itoa(len(rf.peers)))
				if num > len(rf.peers)/2 {
					rf.commitIndex = i
					break
				}
			} else if log[i].Term < rf.currentTerm {
				break
			}
		}
		DPrintf("Func UpdateCommitIndex LEADER:%d commitIndex=%d lastApplied=%d Loglen=%d",rf.me,rf.commitIndex,rf.lastApplied,
			len(log))
		if rf.lastApplied <= rf.commitIndex {
			//panic: runtime error: index out of range [4] with length 4
			/*
				由于是并发执行，lastApplied和commitIndex相比log可能会更新
			*/
			go rf.ApplyStateMachine(&rf.lastApplied,&rf.commitIndex,log)
		}
	}
	rf.mu.Unlock()

}
func (rf *Raft) WaitAppendEntry(i int,appendArgs *AppendEntryArgs,appendReply *AppendEntryReply) bool{
	ok := rf.peers[i].Call("Raft.AppendEntry",appendArgs,&appendReply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok && rf.state == leader && rf.currentTerm == appendArgs.Term {
		if appendReply.Term > rf.currentTerm {
			rf.currentTerm = appendReply.Term
			rf.state = follower
			rf.voteFor = -1
			rf.voteNum = 0
			rf.ResetHeartBeatTimer()
			rf.persist()
		}else if appendReply.Term == rf.currentTerm {
			//DPrintf("Reply LogIndex %d\n",append_reply.LogIndex)
			/*
				nextIndex[i]: leader当前需要同步follower i的日志索引
				matchIndex[i]: leader当前确定已经同步的follower i的日志索引
			*/
			if !appendReply.Success {
				/*
					Figure8Unreliable2C测试出现了返回-1的情况。。。。
					猜测是leader发送appendRPC给follower i，然后leader转换为follower
					之后该节点再次成为leader，当时follower j返回的RPC信息这时才回到leader，接着便进入该段代码
					需要判断返回的term
				*/
				DPrintf("Func SendAppendEntry AppendFail follower:%d matchIndex[%d]=%d LogIndex=%d",i,i,rf.matchIndex[i],
					appendReply.LogIndex)
				if rf.matchIndex[i] < appendReply.LogIndex  {
					rf.nextIndex[i] = appendReply.LogIndex
					if appendReply.ConflictTerm != -1 {
						for j := appendReply.LogIndex-1; j > rf.matchIndex[i]; j-- {
							if rf.log[j].Term == appendReply.ConflictTerm {
								rf.nextIndex[i] = j
							}
						}
					}
				} else {
					rf.nextIndex[i] = rf.matchIndex[i]
				}
				if rf.nextIndex[i] < 0 {
					DPrintf("!!!nextIndex <0!!!!!\n")
				}
				//DPrintf("AppendFail FOLLOWER:%d matchIndex:%d",i,rf.matchIndex[i])
			} else {
				// 同上要注意'旧'的RPC
				/*
					update matchIndex to be prevLogIndex + len(entries[])
				*/
				DPrintf("Func SendAppendEntry AppendSuccess follower:%d matchIndex[%d]=%d LogIndex=%d",i,i,rf.matchIndex[i],
					appendReply.LogIndex)
				if rf.matchIndex[i] < appendReply.LogIndex-1 {
					rf.matchIndex[i] = appendReply.LogIndex-1
					rf.nextIndex[i] = appendReply.LogIndex
				}
				if rf.nextIndex[i] < 0 {
					DPrintf("nextIndex <0!!!!!\n")
				}
				//DPrintf("AppendSuccess FOLLOWER:%d matchIndex:%d",i,rf.matchIndex[i])
			}
			//不能直接用rf.log更新
			go rf.UpdateCommitIndex(appendArgs.Entries)
		}
	}
	return ok
}

func(rf *Raft) SendAppendEntry() {

	for i := range rf.peers {
		if i != rf.me {
			go func(i int){
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					return
				}
				//DPrintf("---HeartBeat--------%s %d currentterm %d send to follower %d\n",
				//	rf.state,rf.me,rf.currentTerm,i)

				//DPrintf("check %d is preId is %d",i,preId)
				//需要拷贝一个新日志，rpc是异步处理,日志可能会改变
				preId := rf.nextIndex[i]
				/*if preId < 0 {
					DPrintf("*****preId=%d***** follower=%d***\n",preId,rf.me)
				}*/
				var preTerm int
				if preId == len(rf.log) {
					preTerm = -1
				} else {
					preTerm = rf.log[preId].Term
				}

				/*
					panic: runtime error: index out of range [10] with length 6

					goroutine 70751 [running]:
					raft.(*Raft).SendHeartBeat.func1(0xc000509200, 0x3)
						/home/laizhilong/Desktop/goProject/src/raft/raft.go:485 +0x17c2
					created by raft.(*Raft).SendHeartBeat
						/home/laizhilong/Desktop/goProject/src/raft/raft.go:466 +0xaa
					exit status 2

					race: limit on 8128 simultaneously alive goroutines is exceeded, dying
				*/
				entries := make([]Log,len(rf.log))
				copy(entries,rf.log)
				//Logs := make([]Log,len(rf.log)-preId)
				//copy(entries,rf.log[preId:])
				Logs := make([]Log,len(rf.log))
				copy(Logs,rf.log)
				appendArgs := &AppendEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: preId,
					PrevLogTerm:  preTerm,
					Entries: Logs,
					LeaderCommit: rf.commitIndex,
				}
				appendReply := AppendEntryReply{
					Term:    -1,
					Success: false,
					LogIndex: -1,
					ConflictTerm : -1,
				}
				rf.mu.Unlock()
				go rf.WaitAppendEntry(i,appendArgs,&appendReply)
			}(i)
		}
	}
}
func(rf *Raft) SendHeartBeat() {

	for i := range rf.peers {
		if i != rf.me {
			go func(i int){
				rf.mu.Lock()
				if rf.state != leader {
					rf.mu.Unlock()
					return
				}
				//DPrintf("-------HeartBeat-------%s:%d currentTerm %d send to node:%d \n",
				//	rf.state,rf.me,rf.currentTerm,i)

				appendArgs := &AppendEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
				}
				appendReply := AppendEntryReply{
					Term:    -1,
					Success: false,
				}
				rf.mu.Unlock()

				ok := rf.peers[i].Call("Raft.HeartBeat",appendArgs,&appendReply)

				rf.mu.Lock()
				if ok && rf.state == leader && rf.currentTerm == appendArgs.Term {
					// 接受大量rpc消息阻塞
					if appendReply.Term > rf.currentTerm {
						rf.currentTerm = appendReply.Term
						rf.state = follower
						rf.voteFor = -1
						rf.voteNum = 0
						rf.ResetHeartBeatTimer()
						rf.persist()
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}
func (rf *Raft)LeaderElection() {
	// follower increments its current term and transitions to candidate state.
	// It then votes for itself and issues RequestVote RPCs in parallel to each of
	// the other servers in the cluster.
	for i := range rf.peers	{
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				if rf.state != candidate {
					rf.mu.Unlock()
					return
				}
				logIndex,logTerm := rf.GetLastLogIno()
				requestVote := &RequestVoteArgs {
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogTerm: logTerm,
					LastLogIndex: logIndex,
				}
				requestReply := &RequestVoteReply {
					Term:        -1,
					VoteGranted: false,
				}
				rf.mu.Unlock()
				go rf.sendRequestVote(i,requestVote,requestReply)
			}(i)
		}
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//DPrintf("Start RequestVote RPC!!! %d currentterm %d",rf.me,rf.currentTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		rf.mu.Lock()
		DPrintf("%s:%d currentTerm=%d Receive RequestVote RPC!!!  Agree=%t remoteTerm=%d",rf.state,rf.me,
			rf.currentTerm,reply.VoteGranted,reply.Term)
		/*
				注意超时RPC的处理以及当前节点的状态
			 	多个节点返回该信息，这时已经是follower的节点不能再收到这个信息了
			 	可能之后收到的term更大，但是没有必要在这里更新了。可以由heartbeat或者new vote来更新term

		*/
		if args.Term == rf.currentTerm && rf.state == candidate {

			if reply.VoteGranted {
				rf.voteNum += 1
				rf.persist()
				if rf.voteNum > len(rf.peers)/2 {
					// 和607的情况类似
					DPrintf("Success New Leader:%d\n",rf.me)
					//rf.changeLeader <- true
					//DPrintf("New Leader send channel****:%d\n",rf.me)
					//rf.timer.Stop()
					rf.ChangeState(leader)
					rf.ResetSendHeartBeatTimer()
					rf.SendAppendEntry()
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = follower
					rf.voteFor = -1
					rf.voteNum =0
					//rf.timer.Stop()
					rf.persist()
					rf.ResetHeartBeatTimer()
				}
			}
		}
		rf.mu.Unlock()
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := false
	// Your code here (2B).
	rf.mu.Lock()
	if rf.state == leader {
		//leader收到客户消息添加到log
		isLeader = true
		info := Log{
			Term:   rf.currentTerm,
			Command:  command,
		}
		DPrintf("Command:"+string(rf.state)+":"+strconv.Itoa(rf.me)+
			" currentTerm:"+strconv.Itoa(rf.currentTerm))
		term = rf.currentTerm
		rf.log = append(rf.log, info)
		index,_ = rf.GetLastLogIno()
		rf.persist()
		//rf.SendHeartBeat()
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	// 不kill，运行整个test时 goroutine会超出限制
	rf.mu.Lock()
	defer  rf.mu.Unlock()
	rf.timer.Stop()

}
func (rf *Raft) ChangeState(s state) {

	if s == leader {
		rf.state = s
		lastId,_ := rf.GetLastLogIno()
		for i := range rf.peers {
			if i != rf.me {
				rf.nextIndex[i] = lastId+1
				rf.matchIndex[i] = 0
			}
		}
		//rf.persist()
		//rf.timer.Stop()
	}else if s == candidate {
		rf.currentTerm += 1
		rf.voteFor = rf.me
		rf.voteNum = 1
		rf.state = s
		rf.persist()
		//rf.ResetElectionTimer()
	}else {
		rf.voteFor = -1
		rf.voteNum = 0
		rf.state = s
		rf.persist()
		//rf.ResetHeartBeatTimer()
	}

}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

/*
	收到现任 Leader 的心跳请求，如果 AppendEntries 请求参数的任期是过期的(args.Term < currentTerm)，不能重置;
	节点开始了一次选举;
	节点投票给了别的节点(没投的话也不能重置);
*/
func (rf *Raft) ResetElectionTimer() {
	//d := time.Duration(rand.Int63() % 333 + 550) * time.Millisecond
	//200 350
	d := time.Duration(rand.Int63() % 250 + 200) * time.Millisecond
	rf.timer.Reset(d)
}
func (rf *Raft) ResetHeartBeatTimer() {
	d := time.Duration(150) * time.Millisecond
	rf.timer.Reset(d)
}
func (rf *Raft) ResetSendHeartBeatTimer() {
	// 110
	d := time.Duration(50) * time.Millisecond
	rf.timer.Reset(d)
}
/*
	没有缓冲的信道，接收方不接受数据造成发送方阻塞，所以应该把channel设置成有缓存的channel
	重置时间片设置channel不太合理
	old leader发送心跳信息，多个节点往old leader节点的channel发送数据
	old leader接受了一个节点的信息，跳出select
	当其他节点再次往这个channel发送信息的时候，由于old leader已经跳出这段代码，此时这个节点将被阻塞
	// select的目的在于及时重置时间片， 用channel来不合适
	添加timeer对象，当发生agreevote、statechange等情况时及时重置时间片

 	statrchange canntidate->follower or leader
	statechange和heartbert是告诉不需要发起投票了, 发生这两种情况时，由当前节点自己通知而不是其他节点
*/
func (rf *Raft) StartLeaderElection() {
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	rf.LeaderElection()
	select {
	/*case  <- rf.stateChange:
		DPrintf("candidate statechange"+string(rf.state)+strconv.Itoa(rf.me))
	case <- rf.heartBeat[]:
		time.Duration(rand.Intn(200)+350) */

	case <- rf.changeLeader:
		/*rf.mu.Lock()
		DPrintf("*****%s:%d become new Leader!!!",rf.state,rf.me)
		rf.mu.Unlock()*/
	//case <- rf.changeFollower:

	case <- rf.timer.C:   //time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
		rf.mu.Lock()
		/*
		   已经timelimit，成为了leader，发送changeleader信号
		   select选择timelimit，无法再接收changeleader信，导致锁无法释放
		*/
		if rf.state == candidate || rf.state == follower {
			rf.ChangeState(candidate)
			rf.ResetElectionTimer()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartFollower() {
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	/*
		    agreeVote 和 statechange是为了重置时间片,重置时间片直接Reset时间片就行了
			stateChange 应该是当时解决出现old term的follower设置的，感觉没必要删除
	*/
	/*
		rf.mu.Lock()
		rf.ResetHeartBeatTimer()
		rf.mu.Unlock()
	*/
	select {
	/*
		case <- rf.agreeVote:
		case  <- rf.stateChange:
			//DPrintf("follower %d changeState!!!!! ",rf.me)
		case <- rf.heartBeat:
			//DPrintf("follower %d Receive HeartBeat!!!!! ",rf.me)
			//time.After(150 * time.Millisecond)
	*/
	case <- rf.timer.C: //time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
		rf.mu.Lock()
		DPrintf("*****%s %d timeLimit!!!!! ",rf.state,rf.me)
		if rf.state == follower || rf.state == candidate {
			rf.ChangeState(candidate)
			//rf.state = candidate
			rf.ResetElectionTimer()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartAppendEntry() {
	rf.SendAppendEntry()
	select {

	case <- rf.timer.C://time.After(120 * time.Millisecond):
		rf.mu.Lock()
		if rf.state == leader {
			rf.ResetSendHeartBeatTimer()
		}else if rf.state == follower {
			rf.ResetHeartBeatTimer()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) StartHeratBeat() {
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	//The tester requires that the leader send heartbeat RPCs
	//no more than ten times per second.
	// 心跳和日志同步如何分开
	rf.SendHeartBeat()
	select {
	/*
		case <- rf.stopHeartBeat:
		rf.mu.Lock()
		DPrintf("------StopHeatBeat------ %s:%d",rf.state,rf.me)
		rf.state = follower
		rf.mu.Unlock()
		case <- rf.heartBeat:
		DPrintf("old leader Receive HeartBeat!!!!!")
		rf.ChangeState(follower)
	*/
	case <- time.After(150 * time.Millisecond):
	}
}

// 传参应该用指针，lastApplied和commitIndex是有可能会变的,确保只提交一次
func (rf *Raft) ApplyStateMachine(lastApplied,commitIndex *int,log []Log) {
	rf.mu.Lock()
	defer  rf.mu.Unlock()
	if *commitIndex >= len(log) {
		return
	}
	for j := *lastApplied+1; j <= *commitIndex; j++ {
		DPrintf("%s %d at StateMachine LogIndex:%d Command:%d ",rf.state,rf.me,j,log[j].Command)
		if j==0 {
			continue
		}
		rf.applyCh <- ApplyMsg {
			CommandValid: true,
			Command: log[j].Command,
			CommandIndex: j,
		}
	}
	rf.lastApplied = *commitIndex
	/*
		if rf.lastApplied < *commitIndex {
			rf.lastApplied = *commitIndex
		}
	*/
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	//rf.readPersist(persister.ReadRaftState())
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	//rf.heartBeat = make([]chan bool,len(peers))
	//rf.stateChange = make(chan state)
	//rf.agreeVote = make([]chan bool,len(peers))
	//rf.candidateChan = make(chan bool)
	//rf.stateChange = make(chan state)
	rf.stopHeartBeat = make(chan bool)
	rf.changeLeader = make(chan bool)
	rf.currentTerm = 0
	rf.voteFor = -1
	//rf.changeFollower = make(chan bool)
	//rf.stopAppendEntry = make(chan bool)
	rf.state = follower
	rf.log = append(rf.log,Log{"",-2})
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.voteNum = 0
	rf.nextIndex= make([]int,len(peers))
	rf.matchIndex = make([]int,len(peers))
	rf.applyCh = applyCh
	rf.timer = time.NewTimer(time.Duration(rand.Int63() % 200) * time.Millisecond)
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	/*
		一直产生阻塞的原因是因为channel发送消息时，由于节点状态改变无法再运行那个select代码块，
		或者rpc返回消息时，channel接受了多个消息阻塞
	*/

	/*go func(){
		for{
			var nowState state
			rf.mu.Lock()
			nowState = rf.state
			rf.mu.Unlock()
			if nowState == leader {
				rf.StartHeratBeat()
			}
		}
	}()*/
	go func() {
		for {
			select {

			//case <- rf.changeLeader:


			case <- rf.timer.C:   //time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
				rf.mu.Lock()
				/*
				   已经timelimit，成为了leader，发送changeleader信号
				   select选择timelimit，无法再接收changeleader信，导致锁无法释放
				*/
				if rf.state == candidate || rf.state == follower {
					rf.ChangeState(candidate)
					rf.LeaderElection()
					rf.ResetElectionTimer()
					DPrintf("*****%s %d timeLimit!!!!! ",rf.state,rf.me)

				}else if rf.state == leader {
					rf.SendAppendEntry()
					rf.ResetSendHeartBeatTimer()
				}
				rf.mu.Unlock()
			}
		}
	}()
	/*go func() {
	for {
		/*
		DPrintf("\n")
		DPrintf("%s %d CurrentTerm:%d CommitIndex:%d ",rf.state,rf.me,rf.currentTerm,rf.commitIndex)
		for i:= range rf.log {
			DPrintf("%s %d CurrentTerm:%d VoteFor %d \n LogId:%d LogCommand:%s LogTerm:%d",
				rf.state,rf.me,rf.currentTerm,rf.voteFor,i+1,rf.log[i].Command,rf.log[i].Term)
		}
		DPrintf("\n")
	*/
	/*var nowState state
			rf.mu.Lock()
			nowState = rf.state
			rf.mu.Unlock()

			if nowState == candidate {
				rf.StartLeaderElection()
			} else if nowState == follower {
				rf.StartFollower()
			} else if nowState == leader {
				rf.StartAppendEntry()
			}
		}
	}()*/
	return rf
}
