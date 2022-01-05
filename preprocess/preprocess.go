package preprocess

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const pathPrefix = "./static/"
const pathAppendix = ".txt"
const pathOriginal = "./static/prideAndPrejudice.txt"

const splitSep = "CHAPTER "

// SplitOriginalTxt is used for splitting the original txt by chapters; should only be used once.
func SplitOriginalTxt() {
	byt, err := ioutil.ReadFile(pathOriginal)
	if err != nil {
		fmt.Println(err)
		return
	}

	s := string(byt)

	lists := strings.Split(s, splitSep)

	index := 1

	for _, str := range lists {
		if len(str) == 0 {
			continue
		}
		newFilename := pathPrefix + strconv.Itoa(index) + pathAppendix
		err = os.WriteFile(newFilename, []byte(str), 0644)
		if err != nil {
			fmt.Println("error for index ", index)
			fmt.Println(err)
		}
		index ++
	}
}

