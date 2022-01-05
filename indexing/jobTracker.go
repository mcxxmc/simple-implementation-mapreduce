package indexing

import "strconv"

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
	NumType1Worker int
	NumType2Worker int
	type1WorkerPool map[int]*TaskTracker1
	type2WorkerPool map[int]*TaskTracker2
	channel1 chan *CompletedWork1
	channel2 chan *CompletedWork2
	jobs []string
}

// Initialize feeds the task plan to the job tracker. Must be called first.
func (jt *JobTracker) Initialize(paths []string) {
	jt.jobs = paths
}

// Run starts the jobs.
func (jt *JobTracker) Run() {
	//TODO: better job allocation by monitoring which worker is free or which one has the slightest load
	for _, worker1 := range jt.type1WorkerPool {
		go worker1.Run()
	}
	for _, worker2 := range jt.type2WorkerPool {
		go worker2.Run()
	}

	// allocate the first batch of jobs
	worker1Id := 0
	for _, job := range jt.jobs {
		jt.type1WorkerPool[worker1Id].Pending <- job
		worker1Id ++
		if worker1Id >= jt.NumType1Worker {
			worker1Id = 0
		}
	}

	//worker2Id := jt.NumType1Worker
	//reservedWorker2Id := worker2Id

	for jt.NumType1Worker > 0 && jt.NumType2Worker > 0 {
		break
	}
}

func NewJobTracker(num1, num2 int) *JobTracker {
	jt := &JobTracker{
		NumType1Worker: num1, NumType2Worker: num2,
		type1WorkerPool: make(map[int]*TaskTracker1),
		type2WorkerPool: make(map[int]*TaskTracker2),
		channel1: make(chan *CompletedWork1, channelSizeJobTracker),
		channel2: make(chan *CompletedWork2, channelSizeJobTracker),
	}

	for i := 0; i < num1; i ++ {
		jt.type1WorkerPool[i] = NewTaskTracker1(i, jt.channel1)
	}
	for i := 0; i < num2; i ++ {
		jt.type2WorkerPool[i + num1] = NewTaskTracker2(i + num1, jt.channel2)
	}

	return jt
}
