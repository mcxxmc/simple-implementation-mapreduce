package test

import (
	"fmt"
	"simple-implementation-mapreduce/indexing"
	"testing"
	"time"
)

const path1TestWorker1 = "../static/1.txt"
const path2TestWorker1 = "../static/2.txt"

// tests with single worker1 and single worker2.
func TestWorker1And2(t *testing.T) {
	completed1 := make(chan *indexing.CompletedWork1, 4)
	worker1 := indexing.NewTaskTracker1(0, completed1)
	completed2 := make(chan *indexing.CompletedWork2, 4)
	worker2 := indexing.NewTaskTracker2(1, completed2)

	go worker1.Run()
	go worker2.Run()

	worker1.Pending <- path1TestWorker1
	result1 := <- completed1
	if result1.Filename != path1TestWorker1 || result1.From != 0 {
		t.Error("mismatched information type1")
	}
	data1 := result1.Collection
	fmt.Println(data1)

	worker2.Pending <- result1
	worker2.Msg <- indexing.MsgOrderingData
	inverted1 := <- completed2
	if inverted1.From != 1 {
		t.Error("mismatched information type2")
	}
	data2 := inverted1.Inverted
	fmt.Println(data2)

	worker1.Pending <- path2TestWorker1
	result2 := <- completed1
	if result2.Filename != path2TestWorker1 || result2.From != 0 {
		t.Error("mismatched information type1")
	}
	data1 = result2.Collection
	fmt.Println(data1)

	worker2.Pending <- result2
	worker2.Msg <- indexing.MsgOrderingData
	inverted2 := <- completed2
	if inverted2.From != 1 {
		t.Error("mismatched information type2")
	}
	data2 = inverted2.Inverted
	fmt.Println(data2)

	worker1.Pending <- indexing.MsgDismissWorker
	worker2.Msg <- indexing.MsgDismissWorker

	time.Sleep(time.Second)
}
