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
	"labgob"
	"labrpc"
	"math/rand"
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
	heartBeat []chan bool
	stateChange []chan state
	agreeVote []chan bool
	candidateChan chan struct{}
	applyCh chan ApplyMsg
	timer *time.Timer
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
	/*DPrintf("%s %d currentterm %d args  %d currentterm %d",
		rf.state,rf.me,rf.currentTerm,args.CandidateId,args.Term)*/

    // 由于当选的leader需要最新的日志，所以即使当前节点已经投过票也需要再次比较follower是否有最新的日志
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
	//DPrintf(strconv.Itoa(args.Term)+"----------------"+strconv.Itoa(rf.currentTerm))
	if args.Term > rf.currentTerm{
		rf.currentTerm= args.Term
		//1个chan时可能同时发生超时、状态变换导致管道阻塞 当时错误地理解了channel。。。。
		rf.ChangeState(follower)
		//rf.stateChange <- follower
		rf.ResetTimer()
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
		//rf.agreeVote <- true
		rf.ResetTimer()
		rf.persist()
		return
	}
	if lastLogId  > args.LastLogIndex {
		return
	}
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	//rf.agreeVote <- true
	rf.ResetTimer()
	rf.persist()
	return
}
func (rf *Raft) AppendEntry(args *AppendEntryArgs) {

}
func (rf *Raft) HeartBeat(args *AppendEntryArgs,reply *AppendEntryReply) {
	//DPrintf("%s %d receive HeartBeat!!!!!!",rf.state,rf.me)
	rf.mu.Lock()
	defer  rf.mu.Unlock()
    /*
    	heartbeat 时为什么不需要比较最后一条日志的索引和log term？？
     */
	if rf.currentTerm > args.Term  {
		reply.Term = rf.currentTerm
		reply.Success = false
	}else {
		if rf.state == candidate {
			rf.ChangeState(follower)
			rf.candidateChan <- struct{}{}
		} else if rf.state == leader {
			rf.ChangeState(follower)
		} else if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.ChangeState(follower)
		}
		rf.currentTerm=args.Term
		reply.Success = false
		//DPrintf("%s %d has passed heartbeat",rf.state,rf.me)
		//  rf.heartBeat <- true 修改为重置时间片即可
		rf.ResetTimer()
		lastId,lastTerm:= rf.GetLastLogIno()
		judge :=func(a *int,b *int) int{
			if *a > *b {
				return *b
			}
			return *a
		}
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
		DPrintf("LogIndex:%d Leader:%d is LogTerm:%d me:%d\n",
			args.PrevLogIndex,args.LeaderId, args.PrevLogTerm,rf.me)
		/*
			args.PrevLogIndex 当前leader想要同步的日志索引
		  reply.LogIndex 检测到不同步的日志索引
		 */
		//当前follower还没有这条日志
		if args.PrevLogIndex > lastId {
			DPrintf("**************************\n")
			//匹配到了最新
			if args.PrevLogIndex == lastId+1 && args.Entries[lastId].Term == lastTerm {
				reply.Success = true
				//符合条件，该日志已经存在,考虑加入没有的日志
				//DPrintf("Leader:"+strconv.Itoa(args.LeaderId))
				//DPrintf("Leader is PrevLogIndex:"+strconv.Itoa(args.PrevLogIndex))
				//可能旧的preLogIndex会传进来
				//if len(args.Entries) > args.PrevLogIndex {
				rf.log = append(rf.log, args.Entries[args.PrevLogIndex])
				rf.persist()
				//}
				lastId,_ := rf.GetLastLogIno()
				reply.LogIndex = lastId+1
				//不能用args.LeaderCommit > rf.commitIndex,原因同下
				minId := judge(&args.PrevLogIndex,&args.LeaderCommit)
				if minId > rf.commitIndex  {
					lastId,_ := rf.GetLastLogIno()
					rf.commitIndex = judge(&lastId,&minId)
				}
				if rf.lastApplied <= rf.commitIndex {
					//.......
					go rf.ApplyStateMachine(&rf.lastApplied,&rf.commitIndex,rf.log)
				}
			}else if args.PrevLogIndex == lastId+1 {
				reply.LogIndex = lastId
				reply.Success = false
			}else {
				reply.LogIndex = lastId+1
				reply.Success = false
			}
			if reply.LogIndex <= 0 {
				DPrintf("Stage 1 appear replyLogIndex <0\n")
			}
			return
		}

		//follower有这条日志但是term不同,删除
		//过最后一个test需要优化：找到当前term的第一个日志发送给leader
		DPrintf("LogIndex:%d Leader:%d is LogTerm:%d \n follower %d is LogTerm is %d",
		args.PrevLogIndex,args.LeaderId, args.PrevLogTerm,rf.me,rf.log[args.PrevLogIndex].Term)

		if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			term := rf.log[args.PrevLogIndex].Term
			reply.Success=false
			//rf.log = rf.log[:args.PrevLogIndex]
			//rf.persist()
			reply.LogIndex = args.PrevLogIndex
			for i := args.PrevLogIndex-1; i>=0 ; i-- {
				if rf.log[i].Term != term {
					reply.LogIndex = i+1
					break
				}
			}
			if reply.LogIndex <= 0 {
				DPrintf("Stage 2 appear replyLogIndex <0\n")
			}
			return
		}

		//优化 leader
		lastId,_ = rf.GetLastLogIno()
		reply.LogIndex = args.PrevLogIndex+1
		reply.Success = true
		for i:= args.PrevLogIndex ; i< len(args.Entries) ; i++ {
			if lastId < i {
				for j:= i ; j< len(args.Entries) ; j++{
					rf.log= append(rf.log,args.Entries[j])
				}
				rf.persist()
				reply.LogIndex = len(args.Entries)
				break
			}
			if rf.log[i].Term != args.Entries[i].Term {
				reply.Success =false
				rf.log = rf.log[:i]
				for j:= i ; j< len(args.Entries) ; j++{
					rf.log= append(rf.log,args.Entries[j])
				}
				rf.persist()
				reply.LogIndex = len(args.Entries)
				break
			}
			reply.LogIndex = i+1
		}
		if reply.LogIndex <= 0 {
			DPrintf("Stage 3 appear replyLogIndex <0\n")
		}

		//注意prevLogIndex可能大于leadercommit,leadercommit不是最新
		//minId := judge(&args.PrevLogIndex,&args.LeaderCommit)
		if args.LeaderCommit > rf.commitIndex  {
			lastId,_ := rf.GetLastLogIno()
			rf.commitIndex = judge(&lastId,&args.LeaderCommit)
		}
		if rf.lastApplied <= rf.commitIndex {
			go rf.ApplyStateMachine(&rf.lastApplied,&rf.commitIndex,rf.log)
		}
	}

}


func (rf *Raft) UpdateCommitIndex (args *AppendEntryArgs) {

	/*
		大多数的日志已经一致,更新commitedIndex
		leader只允许commit当前term的entry，其实是指积压着之前已经被majority认可的entry，
		直到当前term也被majority认可，然后统一commit。
	 */
	rf.mu.Lock()
	if rf.state == leader {
		//DPrintf("*******************************************%d",len(args.Entries))
		//DPrintf(string(rf.state)+" "+strconv.Itoa(rf.me)+" currentTerm:"+strconv.Itoa(rf.currentTerm)+"CommitInedx:"+strconv.Itoa(rf.commitIndex))
		for i:=len(args.Entries)-1; i > rf.commitIndex; i-- {
			//DPrintf("LogId: "+strconv.Itoa(i+1)+" LogTerm:"+strconv.Itoa(args.Entries[i].Term))
			if args.Entries[i].Term == rf.currentTerm {
				num :=1
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
			}
		}
		if rf.lastApplied <= rf.commitIndex {
			go rf.ApplyStateMachine(&rf.lastApplied,&rf.commitIndex,args.Entries)
		}
	}
	rf.mu.Unlock()

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
				//DPrintf("---HeartBeat--------%s %d currentterm %d send to follower %d\n",
				//	rf.state,rf.me,rf.currentTerm,i)
				preId := rf.nextIndex[i]-1
				if preId < 0 {
					DPrintf("*****preId=%d***** follower=%d***\n",preId,rf.me)
				}
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
				if ok && rf.state == leader {
					if append_reply.Term > rf.currentTerm {
						rf.currentTerm = append_reply.Term
						if rf.state != follower {
							rf.ChangeState(follower)
							// 不需要rf.stateChange <- follower，因为leader没有重置时间片的必要
						}
					}else {
						//DPrintf("Reply LogIndex %d\n",append_reply.LogIndex)
						/*
							nextIndex[i]: leader当前需要同步follower i的日志索引
							matchIndex[i]: leader当前确定已经同步的follower i的日志索引
						 */
						if !append_reply.Success {
							/*
								Figure8Unreliable2C测试出现了返回-1的情况。。。。
								猜测是leader发送appendRPC给follower i，然后leader转换为follower
								之后该节点再次成为leader，当时follower j返回的RPC信息这时才回到leader，接着便进入该段代码
							*/
							rf.nextIndex[i] = append_reply.LogIndex
							if rf.nextIndex[i] < 0 {
								DPrintf("!!!nextIndex <0!!!!!\n")
							}
						}else {
							// 同上要注意'旧'的RPC
							/*
							update matchIndex to be prevLogIndex + len(entries[])
							*/
							if rf.matchIndex[i] < append_reply.LogIndex-1 {
								rf.matchIndex[i] = append_reply.LogIndex - 1
								rf.nextIndex[i] = append_reply.LogIndex
							}
							if rf.nextIndex[i] < 0 {
								DPrintf("nextIndex <0!!!!!\n")
							}
						}
						//不能直接用rf.log更新
						go rf.UpdateCommitIndex(append_args)
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
				logIdenx,logTerm := rf.GetLastLogIno()
				request_vote := &RequestVoteArgs {
					Term: rf.currentTerm,
					CandidateId: rf.me,
					LastLogTerm: logTerm,
					LastLogIndex: logIdenx,
				}
				request_reply := RequestVoteReply {
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
		if reply.Term > rf.currentTerm {
			// 多个节点返回该信息，这时已经是follower的节点不能再收到这个信息了
			// 可能之后收到的term更大，但是没有必要在这里更新了。可以由heartbeat或者new vote来更新term
			if rf.state == candidate {
				rf.currentTerm = reply.Term
				rf.ChangeState(follower)
				rf.candidateChan <- struct{}{}
				//rf.stateChange <- follower
			}
		}else if reply.VoteGranted && rf.state==candidate {
			rf.voteNum +=1
			if rf.voteNum >= len(rf.peers)/2+1 {
				// 和607的情况类似
				if rf.state != leader {
					//DPrintf("success new leader :"+strconv.Itoa(rf.me))
					rf.ChangeState(leader)
					rf.candidateChan <- struct{}{}
					//rf.stateChange <- leader
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
		//DPrintf(string(rf.state)+":"+strconv.Itoa(rf.me)+" currentterm:"+strconv.Itoa(rf.currentTerm))
		//DPrintf("command %s",command)
		term = rf.currentTerm
		index = len(rf.log)
		rf.log = append(rf.log, info)
		rf.persist()
		rf.SendHeartBeat()
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
func (rf *Raft) ChangeState(s state) {

	if s == leader {
		rf.state =s
		last_id,_ := rf.GetLastLogIno()
		for i:= range rf.peers {
			if i != rf.me {
				rf.nextIndex[i] = last_id+1
				rf.matchIndex[i] = 0
			}
		}
	}else if s == candidate {
		rf.currentTerm +=1
		rf.voteFor =rf.me
		rf.voteNum =1
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
	rf.persist()
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
func (rf *Raft) ResetTimer() {
	d := time.Duration(rand.Int63() % 333 + 550) * time.Millisecond
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
	statechange和heartbe是告诉不需要发起投票了, 发生这两种情况时，由当前节点自己通知而不是其他节点
*/
func (rf *Raft) StartLeaderElection() {
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	go rf.LeaderElection()
	rf.ResetTimer()
	select {
		/*case  <- rf.stateChange:
			DPrintf("candidate statechange"+string(rf.state)+strconv.Itoa(rf.me))
		case <- rf.heartBeat[]:
			time.Duration(rand.Intn(200)+350) */

		case <- rf.candidateChan:
			DPrintf("Candidate change to %s ID:%d\n",string(rf.state),rf.me)
		case <- rf.timer.C:   //time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
			rf.mu.Lock()
			rf.ChangeState(candidate)
			rf.mu.Unlock()
	}
}


func (rf *Raft) StartFollower() {
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	/*
	    agreeVote 和 statechange是为了重置时间片,重置时间片直接Reset时间片就行了
		stateChange 应该是当时解决出现old term的follower设置的，感觉没必要删除
	 */
	rf.ResetTimer()
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
			//	DPrintf("follower %d timeLimit!!!!! ",rf.me)
			rf.mu.Lock()
			rf.ChangeState(candidate)
			rf.mu.Unlock()
	}
}

func (rf *Raft) StartAppendEntry() {
	rf.StartAppendEntry()
	select {
		//case  <- rf.stateChange:
			//rf.ChangeState(s)

		//case <- rf.heartBeat:
			//	DPrintf("old leader Receive HeartBeat!!!!!")
			//rf.ChangeState(follower)

		case <- time.After(150 * time.Millisecond):
	}
}

func (rf *Raft) StartHeratBeat() {
	//DPrintf("id:"+strconv.Itoa(rf.me)+","+string(rf.state)+" currenterm"+strconv.Itoa(rf.currentTerm))
	//The tester requires that the leader send heartbeat RPCs
	//no more than ten times per second.
	// 心跳和日志同步如何分开
	rf.SendHeartBeat()
	select {
		//case <- rf.stateChange:
			//rf.ChangeState(s)

		//case <- rf.heartBeat:
			//	DPrintf("old leader Receive HeartBeat!!!!!")
			//rf.ChangeState(follower)

		case <- time.After(70 * time.Millisecond):
	}
}

// 传参应该用指针，lastApplied和commitIndex是有可能会变的
func (rf *Raft) ApplyStateMachine(lastApplied,commitIndex *int,log []Log) {
	rf.mu.Lock()
	defer  rf.mu.Unlock()
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
	//rf.stateChange = make([]chan state,len(peers))
	//rf.agreeVote = make([]chan bool,len(peers))
	rf.candidateChan = make(chan struct{})
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
	rf.timer = time.NewTimer(time.Duration(time.Second))
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
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

	go func() {
		for {
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
