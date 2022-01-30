package search

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"simple-implementation-mapreduce/indexing"
	"strings"
)

type records map[string][]*indexing.Pair

const pathSaveJSON = "./static/pap-inverted-index.json"

// Searcher the search engine; please use the safe constructor NewSearcher().
type Searcher struct {
	records records
}

// NewSearcher returns a new Searcher object.
func NewSearcher() *Searcher {
	return &Searcher{records: make(map[string][]*indexing.Pair)}
}

// Initialize initializes the searcher.
func (searcher *Searcher) Initialize() error {
	bytes, err := os.ReadFile(pathSaveJSON)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bytes, &searcher.records)
	if err != nil {
		return err
	}
	return err
}

// Run runs the searcher; should only be called after Initialize().
func (searcher *Searcher) Run() {
	alive := true
	for alive {
		fmt.Println("Please choose your next action.")
		fmt.Println("Type 'all' to view all the words.")
		fmt.Println("Type 'search [word]' to view the statistics of a certain word.")
		fmt.Println("type 'command exit' to exit the program.")

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan() // use `for scanner.Scan()` to keep reading
		userInput := scanner.Text()

		switch userInput {
		case "all":
			fmt.Println(searcher.ListAllWords())
		case "command exit":
			alive = false
		default:
			args := strings.Split(userInput, " ")
			if len(args) > 1 && args[0] == "search" {
				result := searcher.AccurateWordSearch(args[1])
				fmt.Println(result)
			}
		}
	}
}

// ListAllWords returns a list of all the words.
func (searcher *Searcher) ListAllWords() []string {
	r := make([]string, len(searcher.records))
	index := 0
	for word, _ := range searcher.records {
		r[index] = word
		index ++
	}
	return r
}

// AccurateWordSearch returns the statistics of a certain word.
func (searcher *Searcher) AccurateWordSearch(word string) []*indexing.Pair {
	return searcher.records[word]
}
