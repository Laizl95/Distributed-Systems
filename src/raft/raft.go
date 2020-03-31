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
	"bytes"
	"encoding/gob"
	"math/rand"
	//"strconv"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"



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
	heartBeat chan bool
	stateChange chan state
	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


}

func (rf *Raft) GetLastLogIno() (int,int){
	lastId := len(rf.log)-1
	lastTerm := rf.log[lastId].Term
	return lastId,lastTerm
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
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
	e := gob.NewEncoder(w)
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
	rf.readPersist(rf.persister.ReadRaftState())
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var LastIncludedIndex int
	var LastIncludedTerm int
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	rf.commitIndex = LastIncludedIndex
	rf.lastApplied = LastIncludedIndex
	//rf.log = truncateLog(LastIncludedIndex, LastIncludedTerm, rf.log)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted=false
	//Each server will vote for at most one candidate in a given term,
	//DPrintf("%s %d currentterm %d args  %d currentterm %d",
	//rf.state,rf.me,rf.currentTerm,args.CandidateId,args.Term)

	if args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		reply.Term = rf.currentTerm
		return
	}

	//If the candidate’s current term is smaller than the term,
	if rf.currentTerm > args.Term{
		reply.Term=rf.currentTerm
		return
	}
	//If the term is smaller than the candidate’s current term,
	//DPrintf(strconv.Itoa(args.Term)+"----------------"+strconv.Itoa(rf.currentTerm))
	if args.Term > rf.currentTerm{
		rf.currentTerm= args.Term
		//1个chan时可能同时发生超时、状态变换导致管道阻塞
		rf.ChangeState(follower)
		rf.stateChange <- follower
		//rf.voteFor =-1
		//rf.ChangeState(follower)
	}

	//leader is log must contains all committed entries
	//Raft determines which of two logs is more up-to-date by comparing
	//the index and term of the last entries in the logs.
	reply.Term=rf.currentTerm
	lastLogId,_ :=rf.GetLastLogIno()
	lastLog :=rf.log[lastLogId]

	if lastLog.Term > args.LastLogTerm{
		return
	}

	if lastLog.Term < args.LastLogTerm {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		return
	}
	if lastLogId  > args.LastLogIndex {
		return
	}
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	return
}

func (rf *Raft) HeartBeat(args *AppendEntryArgs,reply *AppendEntryReply){
	//DPrintf("%s %d receive HeartBeat!!!!!!",rf.state,rf.me)
	rf.mu.Lock()
	defer  rf.mu.Unlock()

	if rf.currentTerm > args.Term  {
		reply.Term = rf.currentTerm
		reply.Success = false
	}else{
		if rf.currentTerm < args.Term {
			rf.currentTerm=args.Term
			rf.ChangeState(follower)
		}else if rf.state!=follower{
			rf.ChangeState(follower)
		}
		rf.currentTerm=args.Term
		reply.Success = false
		//DPrintf("%s %d has passed heartbeat",rf.state,rf.me)
		rf.heartBeat <- true
		lastId,lastTerm:= rf.GetLastLogIno()
		judge :=func(a *int,b *int) int{
			if *a > *b {
				return *b
			}
			return *a
		}

		//if args.PrevLogIndex == len(args.Entries)
		//当前follower还没有这条日志
		if args.PrevLogIndex > lastId {
			//匹配到了最新
			if args.PrevLogIndex == lastId+1 && args.Entries[lastId].Term == lastTerm {
				reply.Success = true
				//符合条件，该日志已经存在,考虑加入没有的日志
				//DPrintf("Leader:"+strconv.Itoa(args.LeaderId))
				//DPrintf("Leader is PrevLogIndex:"+strconv.Itoa(args.PrevLogIndex))
				//可能旧的preLogIndex会传进来
				if len(args.Entries) > args.PrevLogIndex {
					rf.log = append(rf.log, args.Entries[args.PrevLogIndex])
				}
				//reply.LogIndex = lastId+1
				//不能用args.LeaderCommit > rf.commitIndex,原因同下
				minId := judge(&args.PrevLogIndex,&args.LeaderCommit)
				if minId > rf.commitIndex  {
					lastId,_ := rf.GetLastLogIno()
					rf.commitIndex = judge(&lastId,&minId)
				}
				if rf.lastApplied <= rf.commitIndex {
					rf.ApplyStateMachine(rf.lastApplied,rf.commitIndex,rf.log)
				}
			}else {
				//需要优化减少rpc的次数
				reply.LogIndex = lastId+1
				//if  args.Entries[lastId].Term != lastTerm{
				//	reply.LogIndex = lastId
				//}
				reply.Success = false
			}

			return
		}
		reply.Success = true
		//follower有这条日志但是term不同,删除
		//过最后一个test需要优化：找到当前term的第一个日志发送给leader
		/*DPrintf("LogIndex %d Leader %d is LogTerm %d follower %d is LogTerm is %d",
		args.PrevLogIndex,args.LeaderId, args.PrevLogTerm,rf.me,rf.log[args.PrevLogIndex].Term)*/
		if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {

			term := rf.log[args.PrevLogIndex].Term
			reply.Success=false
			rf.log = rf.log[:args.PrevLogIndex]
			reply.LogIndex = -1
			for i := args.PrevLogIndex-1; i>=0 ; i-- {
				if rf.log[i].Term == term {
					//reply.LogIndex = i
				}else{
					reply.LogIndex = i+1//+2
					break
				}
			}
			return

		}
		//优化 leader
		/*lastId,_ = rf.GetLastLogIno()
		reply.LogIndex = -1
		for i:= args.PrevLogIndex ; i< len(args.Entries) ; i++ {
			if lastId < i {
				reply.LogIndex =i+1
				break
			}
			if rf.log[i].Term != args.Entries[i].Term {
				reply.LogIndex = i+1
				break
			}
		}
		if reply.LogIndex == -1 {
			reply.LogIndex = len(args.Entries)
		}*/
		//rf.commitIndex = args.LeaderCommit
		//不能用args.LeaderCommit > rf.commitIndex,原因同下

		//reply.LogIndex = args.PrevLogIndex+2
		////考虑旧的leader上线，新的leader心跳没有传过来，旧的leader抢了新日志
		//leadercommit已经是最新,follower（old leader）抢了一条新日志长度和leader一样这时候出错
		//注意prevLogIndex可能大于leadercommit,leadercommit不是最新
		minId := judge(&args.PrevLogIndex,&args.LeaderCommit)
		if minId > rf.commitIndex  {
			lastId,_ := rf.GetLastLogIno()
			rf.commitIndex = judge(&lastId,&minId)
		}
		if rf.lastApplied <= rf.commitIndex {
			rf.ApplyStateMachine(rf.lastApplied,rf.commitIndex,rf.log)
		}
	}

}


func (rf *Raft) UpdateCommitIndex (args *AppendEntryArgs) {
	//大多数的日志已经一致,更新commitedIndex
	//commit的时候并不需要发给followers，提交就是回复client。
	//leader只允许commit当前term的entry，其实是指积压着之前已经被majority认可的entry，
	//直到当前term也被majority认可，然后统一commit。
	//rf.mu.Lock()
	if rf.state ==leader {
		//DPrintf("*******************************************%d",len(args.Entries))
		//DPrintf(string(rf.state)+" "+strconv.Itoa(rf.me)+" currentTerm:"+strconv.Itoa(rf.currentTerm)+"CommitInedx:"+strconv.Itoa(rf.commitIndex))
		for i:=len(args.Entries)-1; i> rf.commitIndex ; i-- {
			//DPrintf("LogId: "+strconv.Itoa(i+1)+" LogTerm:"+strconv.Itoa(args.Entries[i].Term))
			if args.Entries[i].Term == rf.currentTerm {
				num :=1
				for j := range rf.peers {
					if j != rf.me {
						//	DPrintf(strconv.Itoa(j)+":"+strconv.Itoa(rf.matchIndex[j]))
						if rf.matchIndex[j] == i{
							num++
						}
					}
				}
				//DPrintf("num="+strconv.Itoa(num)+" len="+strconv.Itoa(len(rf.peers)))
				if num > len(rf.peers)/2 {
					rf.commitIndex = i
					break
				}
			}
		}
		if rf.lastApplied <= rf.commitIndex {
			rf.ApplyStateMachine(rf.lastApplied,rf.commitIndex,args.Entries)
		}
	}
	/*if rf.state ==leader {
		DPrintf("*******************************************%d",len(rf.log))
		DPrintf(string(rf.state)+" "+strconv.Itoa(rf.me)+" currentTerm:"+strconv.Itoa(rf.currentTerm)+"CommitInedx:"+strconv.Itoa(rf.commitIndex))
		for i:=len(rf.log)-1; i> rf.commitIndex ; i-- {
			DPrintf("LogId: "+strconv.Itoa(i+1)+" LogTerm:"+strconv.Itoa(rf.log[i].Term))
			if rf.log[i].Term == rf.currentTerm {
				num :=1
				for j := range rf.peers {
					if j != rf.me {
						//	DPrintf(strconv.Itoa(j)+":"+strconv.Itoa(rf.matchIndex[j]))
						if rf.matchIndex[j] == i{
							num++
						}
					}
				}
				DPrintf("num="+strconv.Itoa(num)+" len="+strconv.Itoa(len(rf.peers)))
				if num > len(rf.peers)/2 {
					rf.commitIndex = i
					break
				}
			}
		}
	}*/
	//rf.mu.Unlock()
}

func(rf *Raft) SendHeartBeat(){

	for i := range rf.peers{
		if i != rf.me{
			go func(i int){
				rf.mu.Lock()

				//DPrintf("---HeartBeat--------%s %d currentterm %d",rf.state,rf.me,rf.currentTerm)
				preId := rf.nextIndex[i]-1
				//DPrintf("check %d is preId is %d",i,preId)
				//需要拷贝一个新日志，rpc是异步处理,日志可能会改变
				preTerm := rf.log[preId].Term
				entries := make([]Log,len(rf.log))
				copy(entries,rf.log)
				append_args := &AppendEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: preId,
					PrevLogTerm:  preTerm,
					Entries: entries,
					LeaderCommit: rf.commitIndex,
				}
				append_reply := AppendEntryReply{
					Term:    -1,
					Success: false,
					LogIndex:-1,
				}
				rf.mu.Unlock()
				ok := rf.peers[i].Call("Raft.HeartBeat",append_args,&append_reply)
				rf.mu.Lock()
				if ok && rf.state==leader{
					if append_reply.Term > rf.currentTerm{
						rf.currentTerm = append_reply.Term
						rf.ChangeState(follower)
						rf.stateChange <- follower
					}else {

						if !append_reply.Success {
							//可能发送同一个prelogidenx多次都被拒绝
							//rf.nextIndex[i]-=1
							if append_reply.LogIndex == -1{
							rf.nextIndex[i] = append_args.PrevLogIndex
							}else {
								rf.nextIndex[i] = append_reply.LogIndex
							}
						}else {
							//该日志在follower存在，更新matchIdenx
							//bug：leader中nextIdenx目前没有日志
							lens := len(append_args.Entries)
							if rf.matchIndex[i] < append_args.PrevLogIndex  {
								if append_args.PrevLogIndex < lens {
									rf.matchIndex[i] = append_args.PrevLogIndex
								}
							}
							//==lens的时候匹配完毕
							if append_args.PrevLogIndex+1 < lens {
								rf.nextIndex[i] = append_args.PrevLogIndex+2
							}
							//math > next
							/*lens := len(append_args.Entries)
							if rf.matchIndex[i] < append_reply.LogIndex-1 && rf.matchIndex[i]<rf.nextIndex[i] {
								if append_args.PrevLogIndex-1 < lens && append_args.PrevLogIndex-1<rf.nextIndex[i] {
									rf.matchIndex[i] = append_reply.LogIndex-1
								}
							}
							if append_reply.LogIndex <= lens{
								rf.nextIndex[i] = append_reply.LogIndex
							}*/

						}
						//不能直接用rf.log更新
						rf.UpdateCommitIndex(append_args)
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

func (rf *Raft)LeaderElection() {
	// follower increments its current term and transitions to candidate state.
	//It then votes for itself and issues RequestVote RPCs in parallel to each of
	//the other servers in the cluster.
	for i := range rf.peers	{
		if i != rf.me{

			go func(i int) {
				rf.mu.Lock()
				logIdenx,logTerm := rf.GetLastLogIno()
				request_vote := &RequestVoteArgs{
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogTerm: logTerm,
					LastLogIndex: logIdenx,
				}
				request_reply := RequestVoteReply{
					Term:        -1,
					VoteGranted: false,
				}
				rf.mu.Unlock()

				rf.sendRequestVote(i,request_vote,&request_reply)

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
		//DPrintf("Receive RequestVote RPC!!! %d currentterm %d",rf.me,rf.currentTerm)
		rf.mu.Lock()
		if reply.Term > rf.currentTerm{
			rf.currentTerm = reply.Term
			rf.ChangeState(follower)
			rf.stateChange <- follower
		}else if reply.VoteGranted && rf.state==candidate{
			rf.voteNum +=1
			if rf.voteNum >= len(rf.peers)/2+1{
				//DPrintf("success new leader :"+strconv.Itoa(rf.me))
				rf.ChangeState(leader)
				rf.stateChange <- leader
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
		//DPrintf(string(rf.state)+":"+strconv.Itoa(rf.me)+" currentterm:"+strconv.Itoa(rf.currentTerm))
		//DPrintf("command %s",command)
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, info)
		rf.SendHeartBeat()
		//rf.ConsistencyEntries()
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
}
func (rf *Raft) ChangeState(s state){

	if s== leader {
		rf.state =s
		last_id,_ := rf.GetLastLogIno()

		for i:= range rf.peers {
			if i != rf.me {
				rf.nextIndex[i] = last_id+1
				rf.matchIndex[i] = 0
			}
		}
	}else if s==candidate {
		rf.currentTerm +=1
		rf.voteFor =rf.me
		//if rf.state == follower {
		rf.voteNum =1
		//	}
		rf.state=s
	}else {
		//leader1 shi candidate 2.-1
		///candidate candidate 1.vote
		//candidate -> leader2
		//leader1 -> follower

		rf.voteFor = -1
		rf.voteNum =0

		rf.state = s
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
func (rf *Raft) StartLeaderElection() {
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	rf.LeaderElection()
	select {

	case  <- rf.stateChange:
		//DPrintf("candidate statechange"+string(rf.state)+strconv.Itoa(rf.me))
		//不需要锁,前面已经锁了
		//rf.ChangeState(s)

	case <- rf.heartBeat:
		//rf.ChangeState(follower)
		//time.Duration(rand.Intn(200)+350) *
		//time.Millisecond
	case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
		rf.mu.Lock()
		rf.ChangeState(candidate)
		rf.mu.Unlock()
	}
}
func (rf *Raft) StartFollower(){
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	select {

	case  <- rf.stateChange:
		//DPrintf("follower %d changeState!!!!! ",rf.me)
		//rf.ChangeState(s)

	case <-  rf.heartBeat:
		//DPrintf("follower %d Receive HeartBeat!!!!! ",rf.me)
		//time.After(150 * time.Millisecond)
	case <-time.After( time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
		//	DPrintf("follower %d timeLimit!!!!! ",rf.me)
		rf.mu.Lock()
		rf.ChangeState(candidate)
		rf.mu.Unlock()
	}

}
func (rf *Raft) StartHeratBeat(){
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	//The tester requires that the leader send heartbeat RPCs
	//no more than ten times per second.
	rf.SendHeartBeat()
	select {
	case  <- rf.stateChange:
		//rf.ChangeState(s)

	case <- rf.heartBeat:
		//	DPrintf("old leader Receive HeartBeat!!!!!")
		//rf.ChangeState(follower)

	case <- time.After(50 * time.Millisecond):

	}
}

/*func (rf *Raft) ApplyStateMachine(log []Log,lastApplied,commitIndex int) {
	rf.mu.Lock()
	defer  rf.mu.Unlock()
	for j:=lastApplied; j<=commitIndex ;j++{
		if j==0 {
			continue
		}
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      log[j].Command,
			CommandIndex: j,
		}
	}
	if rf.lastApplied < commitIndex+1{
		rf.lastApplied = commitIndex+1
	}

}*/
func (rf *Raft) ApplyStateMachine(lastApplied,commitIndex int,log []Log) {
	//rf.mu.Lock()
	//defer  rf.mu.Unlock()
	for j:=lastApplied; j<=commitIndex ;j++{
		DPrintf("%s %d at StateMachine LogIndex:%d Command:%d ",rf.state,rf.me,j+1,log[j].Command)
		if j==0 {
			continue
		}
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      log[j].Command,
			CommandIndex: j,
		}
	}
	if rf.lastApplied < commitIndex+1{
		rf.lastApplied = commitIndex+1
	}
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.readPersist(persister.ReadRaftState())
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartBeat = make(chan bool,1000)
	rf.stateChange = make(chan state,1000)
	rf.state = follower
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = append(rf.log,Log{"",0})
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.voteNum = 0
	rf.nextIndex= make([]int,len(peers))
	rf.matchIndex = make([]int,len(peers))
	rf.applyCh = applyCh
	/*go func(){
		for{
			rf.ApplyStateMachine()
		}
	}()*/
	/*go func(){
		for{
			if rf.state == leader {
				rf.StartAppendEntries()
			}
		}
	}()*/

	go func(){
		for{
			/*DPrintf("\n")
			DPrintf("%s %d CurrentTerm:%d CommitIndex:%d ",rf.state,rf.me,rf.currentTerm,rf.commitIndex)
			for i:= range rf.log {
				DPrintf("%s %d CurrentTerm:%d VoteFor %d \n LogId:%d LogCommand:%s LogTerm:%d",
					rf.state,rf.me,rf.currentTerm,rf.voteFor,i+1,rf.log[i].Command,rf.log[i].Term)
			}
			DPrintf("\n")*/
			if rf.state == candidate {
				rf.StartLeaderElection()
			}
			if rf.state == follower {
				rf.StartFollower()
			}
			if rf.state == leader {
				rf.StartHeratBeat()
			}
		}
	}()
	return rf
}
