package indexing

import (
	"fmt"
	"os"
	"sort"
	"time"
)

// TaskTracker1 the type1 worker: count the word frequency of each file
type TaskTracker1 struct {
	Pending   chan string          // the channel waiting for more file(names) for processing
	Completed chan *CompletedWork1 // the channel to send finished work
	Id   int
	Idle bool
}

// Run runs the worker.
func (tk1 *TaskTracker1) Run() {
	for {
		filename := ""
		select {
		case filename = <- tk1.Pending:
			if filename == MsgDismissWorker {
				fmt.Println("task tracker type1 stops, id: ", tk1.Id)
				break
			}
			m := tk1.countWordFreq(filename)
			msg := &CompletedWork1{From: tk1.Id, Filename: filename, Collection: m}
			tk1.Completed <- msg

		case <- time.After(2 * time.Second):
			tk1.Idle = true
			continue
		}
	}
}

// counts the word frequency. Currently, it assumes there is no hash colliding.
func (tk1 *TaskTracker1) countWordFreq(path string) map[string]int {
	r := make(map[string]int)
	byt, err := os.ReadFile(path)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	s := ""
	
	for _, b := range byt {
		switch {
		case b >= '0' && b <= '9' || b == '-' || b >= 'a' && b <= 'z':
			s += string(b)
		case b >= 'A' && b <= 'Z':
			b += 'a' - 'A'
			s += string(b)
		default:
			if s != "" {
				r[s] ++
				s = ""
			}
		}
	}
	
	return r
}

func NewTaskTracker1(id int, completed chan *CompletedWork1) *TaskTracker1 {
	return &TaskTracker1{Id: id, Pending: make(chan string, channelSizeTaskTracker1), Completed: completed}
}

// TaskTracker2 the type2 worker for precessing the data from type1 worker.
// It will only return value to the Completed channel when an ordering message is sent through the Msg channel.
type TaskTracker2 struct {
	Pending   chan *CompletedWork1
	Completed chan *CompletedWork2
	Msg       chan string
	Inverted  map[string][]*pair
	Id        int
	Idle      bool
}

// Run runs the worker.
func (tk2 *TaskTracker2) Run() {
	for {
		select {
		case work1 := <- tk2.Pending:
			tk2.invertAndCombine(work1.Filename, work1.Collection)

		case msg := <- tk2.Msg:
			switch msg {
			case MsgDismissWorker:
				fmt.Println("task tracker type2 stops, id: ", tk2.Id)
				break
			case MsgOrderingData:
				msg := &CompletedWork2{From: tk2.Id, Inverted: tk2.Inverted}
				tk2.Completed <- msg
			case MsgClearingData:
				tk2.Inverted = make(map[string][]*pair)
			}

		case <- time.After(2 * time.Second):
			tk2.Idle = true
			continue
		}
	}
}

func (tk2 *TaskTracker2) invertAndCombine(filename string, data map[string]int) {
	for word, freq := range data {
		pairs, exist := tk2.Inverted[word]
		if !exist {
			pairs = make([]*pair, 0)
		}
		newPair := &pair{Filename: filename, Freq: freq}
		pairs = append(pairs, newPair)
		newIndex := sort.Search(len(pairs) - 1, func(i int) bool {return pairs[i].Freq <= freq})
		for i := len(pairs) - 1; i > newIndex; i -- {
			pairs[i] = pairs[i - 1]
		}
		pairs[newIndex] = newPair

		tk2.Inverted[word] = pairs
	}
}

func NewTaskTracker2(id int, completed chan *CompletedWork2) *TaskTracker2 {
	return &TaskTracker2{
		Id:      id,
		Pending: make(chan *CompletedWork1, channelSizeTaskTracker2),
		Completed: completed,
		Msg: make(chan string, channelTaskTracker2Msg),
		Inverted: make(map[string][]*pair),
	}
}
