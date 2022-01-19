package indexing

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

// TaskTracker1 the type1 worker: count the word frequency of each file
type TaskTracker1 struct {
	Pending   chan string          // the channel waiting for more file(names) for processing
	Completed chan *CompletedWork1 // the channel to send finished work
	Id   int
	Idle bool
	Alive bool
}

// Run runs the worker.
func (tk1 *TaskTracker1) Run() {
	tk1.Alive = true
	for {
		filename := ""
		select {
		case filename = <- tk1.Pending:
			if filename == MsgDismissWorker {
				fmt.Println("task tracker type1 stops, id: ", tk1.Id)
				tk1.Alive = false
				break
			}
			tk1.Idle = false
			fmt.Println("task tracker type1 is working, id: ", tk1.Id, " task name: ", filename)
			m := tk1.countWordFreq(filename)
			msg := &CompletedWork1{From: tk1.Id, Filename: filename, Collection: m}
			tk1.Completed <- msg
			tk1.Idle = true

		case <- time.After(2 * time.Second):
			continue
		}
	}
}

// counts the word frequency. Currently, it assumes there is no hash collision.
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

// TaskTracker2 the type2 worker for processing the data from type1 worker.
// It will only return value to the Completed channel when an ordering message is sent through the Msg channel.
type TaskTracker2 struct {
	Pending   chan *CompletedWork1
	Pending2  chan *CompletedWork2
	Completed chan *CompletedWork2
	Msg       chan string
	Inverted  map[string][]*pair
	Id        int
	Idle      bool
	Alive 	  bool
}

// Run runs the worker.
func (tk2 *TaskTracker2) Run() {
	tk2.Alive = true
	for {
		select {
		case work1 := <- tk2.Pending:
			fmt.Println("task tracker type2 is working, id: ", tk2.Id, " task name: ", work1.Filename)
			tk2.Idle = false
			tk2.invertAndCombine(work1.Filename, work1.Collection)
			tk2.Idle = true

		case work2 := <- tk2.Pending2:
			fmt.Println("task tracker type2 is refining, id: ", tk2.Id)
			tk2.Idle = false
			tk2.union(work2.Inverted)
			tk2.Idle = true

		case msg := <- tk2.Msg:
			switch msg {
			case MsgDismissWorker:
				fmt.Println("task tracker type2 stops, id: ", tk2.Id)
				tk2.Alive = false
				break

			case MsgOrderingData:
				fmt.Println("task tracker type2 is ordered for data, id: ", tk2.Id)
				msg := &CompletedWork2{From: tk2.Id, Inverted: tk2.Inverted}
				tk2.Completed <- msg

			case MsgClearingData:
				fmt.Println("task tracker type2 is clearing its data, id: ", tk2.Id)
				tk2.Inverted = make(map[string][]*pair)

			case MsgSaveData2Disk:
				fmt.Println("task tracker type2 is saving JSON to disk, id: ", tk2.Id)
				b, err := json.Marshal(tk2.Inverted)
				if err != nil {
					fmt.Println(err)
				} else {
					err = ioutil.WriteFile(pathSaveJSON, b, 0644)
					if err != nil {
						fmt.Println(err)
					}
				}
			}

		case <- time.After(2 * time.Second):
			continue
		}
	}
}

// create the inverted index (unsorted).
func (tk2 *TaskTracker2) invertAndCombine(filename string, data map[string]int) {
	for word, freq := range data {
		pairs, exist := tk2.Inverted[word]
		if !exist {
			pairs = make([]*pair, 0)
		}
		newPair := &pair{Filename: filename, Freq: freq}
		pairs = append(pairs, newPair)
		tk2.Inverted[word] = pairs
	}
}

// absorbs a map of inverted index (unsorted).
func (tk2 *TaskTracker2) union(m map[string][]*pair) {
	for word, newPairs := range m {
		_, exist := tk2.Inverted[word]
		if !exist {
			tk2.Inverted[word] = newPairs
		} else {
			tk2.Inverted[word] = append(tk2.Inverted[word], newPairs...)
		}
	}
}

func NewTaskTracker2(id int, completed chan *CompletedWork2) *TaskTracker2 {
	return &TaskTracker2{
		Id:      id,
		Pending: make(chan *CompletedWork1, channelSizeTaskTracker2),
		Pending2: make(chan *CompletedWork2, channelSizeTaskTracker2),
		Completed: completed,
		Msg: make(chan string, channelTaskTracker2Msg),
		Inverted: make(map[string][]*pair),
	}
}
