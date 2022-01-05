package indexing

import (
	"fmt"
	"os"
	"time"
)

// the type1 worker: count the word frequency of each file
type taskTracker1 struct {
	id int
	pending chan string  // the channel waiting for more file(names) for processing
	completed chan *completedWork1  // the channel to send finished work
}

// executes.
func (tk1 *taskTracker1) run() {
	for {
		filename := ""
		select {
		case filename = <- tk1.pending:
			if filename == dismissWorker {
				fmt.Println("task tracker type1 stops, id: ", tk1.id)
				break
			}
			m := tk1.countWordFreq(filename)
			msg := &completedWork1{from: tk1.id, filename: filename, collection: m}
			tk1.completed <- msg

		case <- time.After(2 * time.Second):
			continue
		}
	}
}

// counts the word frequency. Currently, it assumes there is no hash colliding.
func (tk1 *taskTracker1) countWordFreq(path string) map[string]int {
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

func NewTaskTracker1(id int, completed chan *completedWork1) *taskTracker1 {
	return &taskTracker1{id: id, pending: make(chan string, channelSizeTaskTracker1), completed: completed}
}

// the type2 worker for precessing the data from type1 worker.
type taskTracker2 struct {
	id int
	pending chan *completedWork1
	completed chan *completedWork2
}

func NewTaskTracker2(id int, completed chan *completedWork2) *taskTracker2 {
	return &taskTracker2{id: id, pending: make(chan *completedWork1, channelSizeTaskTracker2), completed: completed}
}
