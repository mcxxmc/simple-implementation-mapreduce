package indexing

type pair struct {
	Freq     int    `json:"freq"`
	Filename string `json:"filename"`
}

// the type1 finished work, which is the word frequency of each file.
type completedWork1 struct {
	from int
	filename string
	collection map[string]int
}

// the type2 finished work, which is the inverted index for some files.
type completedWork2 struct {
	from int
	inverted map[string][]*pair
}

const dismissWorker = "dismiss"

// JobTracker the manager to allocate jobs. Use NewJobTracker() as the constructor.
type JobTracker struct {
	NumType1Worker int
	NumType2Worker int
	type1WorkerPool map[int]*taskTracker1
	type2WorkerPool map[int]*taskTracker2
	channel1 chan *completedWork1
	channel2 chan *completedWork2
}

func NewJobTracker(num1, num2 int) *JobTracker {
	jt := &JobTracker{
		NumType1Worker: num1, NumType2Worker: num2,
		type1WorkerPool: make(map[int]*taskTracker1),
		type2WorkerPool: make(map[int]*taskTracker2),
		channel1: make(chan *completedWork1, channelSizeJobTracker),
		channel2: make(chan *completedWork2, channelSizeJobTracker),
	}

	for i := 0; i < num1; i ++ {
		jt.type1WorkerPool[i] = NewTaskTracker1(i, jt.channel1)
	}
	for i := 0; i < num2; i ++ {
		jt.type2WorkerPool[i + num1] = NewTaskTracker2(i + num1, jt.channel2)
	}

	return jt
}
