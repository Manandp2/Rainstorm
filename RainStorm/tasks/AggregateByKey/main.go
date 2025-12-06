package main

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
)

func main() {
	groupCol, err := strconv.Atoi(os.Args[1])
	if err != nil {
		return
	}

	reader := csv.NewReader(os.Stdin)

	m := make(map[string]int)

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		if len(line) <= groupCol {
			continue
		}
		m[line[groupCol]] += 1
	}

	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	for k, v := range m {
		// Convert count to string
		countStr := strconv.Itoa(v)
		_ = writer.Write([]string{k, countStr})
	}
}
