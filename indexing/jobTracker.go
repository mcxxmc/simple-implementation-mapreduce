package indexing

import (
	"fmt"
	"strconv"
	"time"
)

type pair struct {
	Freq     int    `json:"freq"`
	Filename string `json:"filename"`
}

func (p *pair) String() string {
	return "[" + strconv.Itoa(p.Freq) + ", " + p.Filename + "]"
}

// CompletedWork1 the type1 finished work, which is the word frequency of each file.
type CompletedWork1 struct {
	From       int
	Filename   string
	Collection map[string]int
}

// CompletedWork2 the type2 finished work, which is the inverted index for some files.
type CompletedWork2 struct {
	From     int
	Inverted map[string][]*pair
}

// JobTracker the manager to allocate jobs. Use NewJobTracker() as the constructor.
type JobTracker struct {
	type1WorkerPool map[int]*TaskTracker1
	type2WorkerPool map[int]*TaskTracker2
	type1WorkerMonitor map[int]int  // 0 means "idle"
	type2WorkerMonitor map[int]int
	channel1 chan *CompletedWork1
	channel2 chan *CompletedWork2
	jobs []string
	NumType1Worker int
	NumType2Worker int
	Alive bool
}

// Initialize feeds the task plan to the job tracker. Must be called first.
func (jt *JobTracker) Initialize(paths []string) {
	jt.jobs = paths
}

// checks if all the workers are idle (or dead) at this moment.
func (jt *JobTracker) allWorkersIdleOrDead() bool {
	for _, n := range jt.type1WorkerMonitor {
		if n > 0 && n < Dead {
			return false
		}
	}

	for _, n := range jt.type2WorkerMonitor {
		if n > 0 && n < Dead {
			return false
		}
	}

	return true
}

// assigns jobs to type1 workers in a round-robin fashion.
func (jt *JobTracker) assignTasksToWorker1RoundRobin() {
	// TODO: change this to a for loop so the manager may accept new jobs
	curIndex := 0
	for _, s := range jt.jobs {
		fmt.Println("manager assigns task ", s, " to type1 worker, worker id: ", curIndex)
		jt.type1WorkerPool[curIndex].Pending <- s
		jt.type1WorkerMonitor[curIndex] ++
		curIndex ++
		if curIndex == jt.NumType1Worker {
			curIndex = 0
		}
	}
	jt.jobs = []string{}  // clean the jobs
}

// finds the most idle workers and returns its id.
func (jt *JobTracker) findMostIdleWorker2() int {
	var id int
	var workload int
	for k, v := range jt.type2WorkerMonitor {
		//TODO: maybe this can be changed to a priority queue
		if v == 0 {
			return k
		}
		if k == jt.NumType1Worker || workload > v {  // the minimum worker2 id is jt.NumType1Worker
			id = k
			workload = v
		}
	}

	if workload == Dead {
		// TODO: raise panic or other error-handling methods here (e.g., "revive" a dead type2 worker)
		fmt.Println("fail to find the most idle type2 worker, will try again after 2 seconds...")
		<- time.After(2 * time.Second)
		id = jt.findMostIdleWorker2()
	}

	return id
}

// get results from the type2 workers if they are idle, except for those in the exception list.
func (jt *JobTracker) fetchResultsFromWorker2s(min int) {
	// TODO: if type2 workers are in fact different machines, this method will not work. (should try other methods e.g. grpc)
	for id, worker2 := range jt.type2WorkerPool {
		if id > min && worker2.Alive && worker2.Idle && jt.type2WorkerMonitor[id] > 0 {
			worker2.Msg <- MsgOrderingData
			jt.type2WorkerMonitor[id] = 0
			worker2.Msg <- MsgClearingData
		}
	}
}

// assigns tasks1 from type1 worker to type2 worker.
func (jt *JobTracker) assignTasksToWorker2() {
	for jt.Alive {
		select {
		case completedWork1 := <- jt.channel1:
			worker1Id := completedWork1.From
			fmt.Println("manager receives type1 completed work from type1 worker, worker id: ", worker1Id)
			jt.type1WorkerMonitor[worker1Id] --

			worker2Id := jt.findMostIdleWorker2()
			fmt.Println("manager assigns new work to type2 worker, worker id: ", worker2Id)
			jt.type2WorkerPool[worker2Id].Pending <- completedWork1
			jt.type2WorkerMonitor[worker2Id] ++

		case <- time.After(2 * time.Second):
			fmt.Println("manager feels lonely")
			continue
		}
	}
}

// Run starts the jobs.
func (jt *JobTracker) Run() {
	//TODO: better job allocation by monitoring which worker is free or which one has the slightest load
	jt.Alive = true

	for _, worker1 := range jt.type1WorkerPool {
		go worker1.Run()
	}
	for _, worker2 := range jt.type2WorkerPool {
		go worker2.Run()
	}

	go jt.assignTasksToWorker1RoundRobin()

	go jt.assignTasksToWorker2()

	leadingWorker2 := jt.NumType1Worker  // the best machine for final results
	lastHelperId := leadingWorker2  // it is important that all worker2 ids are sequentially increasing
	for i := 1; i < jt.NumType2Worker / ratio; i ++ {
		lastHelperId ++
	}
	roundRobinIndex := leadingWorker2
	fmt.Println("manager has put on an elite team of type2 workers")

	for jt.Alive {
		select {
		case completedWork2 := <- jt.channel2:
			// TODO: more flexible management that allows a dynamic numbers of leading type2 workers
			// round-robin
			fmt.Println("manager is redirecting work from type2 worker ", completedWork2.From,
				" to worker ", roundRobinIndex)
			jt.type2WorkerPool[roundRobinIndex].Pending2 <- completedWork2
			jt.type2WorkerMonitor[roundRobinIndex] ++
			roundRobinIndex ++
			if roundRobinIndex > lastHelperId {
				roundRobinIndex = leadingWorker2
			}
		case <- time.After(2 * time.Second):
			if jt.allWorkersIdleOrDead() || leadingWorker2 == lastHelperId {
				fmt.Println("manager is going to leave")
				jt.type2WorkerPool[leadingWorker2].Msg <- MsgSaveData2Disk
				jt.Alive = false
				break
			} else {
				fmt.Println("manager is trying to fetch results from type2 workers")
				jt.fetchResultsFromWorker2s(lastHelperId)
				if lastHelperId > leadingWorker2 {
					fmt.Println("manager is cutting down the size of the elite team")
					lastHelperId = (lastHelperId + leadingWorker2) / 2
				}
			}
		}
	}

	// the idle attribute will only be set to false when the worker receives the order (with a small delay)
	<- time.After(time.Second)
	for !jt.type2WorkerPool[leadingWorker2].Idle {}
}

// NewJobTracker
//
// Returns a new job tracker with #num1 type1 workers and #num2 type2 workers.
func NewJobTracker(num1, num2 int) *JobTracker {
	jt := &JobTracker{
		type1WorkerPool: make(map[int]*TaskTracker1),
		type2WorkerPool: make(map[int]*TaskTracker2),
		type1WorkerMonitor: make(map[int]int),
		type2WorkerMonitor: make(map[int]int),
		channel1: make(chan *CompletedWork1, channelSizeJobTracker),
		channel2: make(chan *CompletedWork2, channelSizeJobTracker),
		NumType1Worker: num1, NumType2Worker: num2,
	}

	for i := 0; i < num1; i ++ {
		jt.type1WorkerPool[i] = NewTaskTracker1(i, jt.channel1)
		jt.type1WorkerMonitor[i] = 0
	}
	for i := 0; i < num2; i ++ {
		jt.type2WorkerPool[i + num1] = NewTaskTracker2(i + num1, jt.channel2)
		jt.type2WorkerMonitor[i + num1] = 0
	}

	return jt
}
