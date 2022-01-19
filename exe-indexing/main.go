package main

import (
	"simple-implementation-mapreduce/indexing"
	"strconv"
)

const chaptersNumber = 61

const pathStaticPrefix = "./static/"
const txtAppendix = ".txt"

func main() {
	jobs := make([]string, chaptersNumber)
	for i := 0; i < chaptersNumber; i ++ {
		jobs[i] = pathStaticPrefix + strconv.Itoa(i + 1) + txtAppendix
	}
	jt := indexing.NewJobTracker(8, 8)
	jt.Initialize(jobs)
	jt.Run()
}
