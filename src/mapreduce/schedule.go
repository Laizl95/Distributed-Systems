package mapreduce

import (
	"fmt"
	"sync"
	"strconv"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	//hand out tasks to the avaliable workers
	//give each worker a sequence of tasks
	//1.通过 registerChan得到worker,chan有所有worker的address
	//2.调用call(worker address,"Worker.DoTask",Worker.DoTask struct,nil),发送doTaskRPC给worker,
	//	DoTaskArgs在common_rpc.go DoTaskArgs中
	var wg sync.WaitGroup
	for i:=0; i<ntasks; i++{
		//debug("now %s ntasks\n",strconv.Itoa(i))
		wg.Add(1)
		go func(i int,phase jobPhase){
			for{
				im :=DoTaskArgs{JobName:jobName,Phase:phase,TaskNumber:i,NumOtherPhase:n_other}
				//debug("now %s mapPhase\n",strconv.Itoa(i))
				if phase == mapPhase{
					im.File = mapFiles[i]
				}
				//当倒数第二个任务完成后重新加到channel中时，已经没有go程会取出这个worker了，
				//此时最后一个go程再想将worker添加到channel中时就会阻塞
				//fmt.Print(im.JobName+" "+string(im.Phase)+string(im.TaskNumber))
				//debug(" now is %s\n",im.JobName+" "+string(im.Phase)+string(im.TaskNumber))

				worker := <- registerChan
				/*go func() {
					registerChan <- worker
				}()*/
				ok := call(worker,"Worker.DoTask",im,new(struct{}))
				if ok {
					debug("nowsssss %s is ok\n",im.JobName+" "+string(im.Phase)+strconv.Itoa(i))
					wg.Done()
					registerChan <- worker
					break
				}
			}
		}(i,phase)
	}
	wg.Wait()
	/*var wg sync.WaitGroup
	for cur:=0;cur<ntasks;cur++{
		wg.Add(1)
		go func(cur int, registerChan chan string) {
			address:=<-registerChan
			call(address,"Worker.DoTask",DoTaskArgs{jobName,mapFiles[cur],phase,cur,n_other},nil)
			wg.Done()
			registerChan<-address
		}(cur,registerChan)
	}
	wg.Wait()*/
	/*type DoTaskArgs struct {
		JobName    string
		File       string   // only for map, the input file
		Phase      jobPhase // are we in mapPhase or reducePhase?
		TaskNumber int      // this task's index in the current phase

		// NumOtherPhase is the total number of tasks in other phase; mappers
		// need this to compute the number of output bins, and reducers needs
		// this to know how many input files to collect.
		NumOtherPhase int
	}*/
	fmt.Printf("Schedule: %v done\n", phase)
}
