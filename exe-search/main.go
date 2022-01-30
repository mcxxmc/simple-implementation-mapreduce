package main

import (
	"fmt"
	"simple-implementation-mapreduce/search"
)

func main() {
	searcher := search.NewSearcher()
	err := searcher.Initialize()
	if err != nil {
		fmt.Println(nil)
		return
	}
	searcher.Run()
}