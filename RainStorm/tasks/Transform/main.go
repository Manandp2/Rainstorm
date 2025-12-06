package main

import (
	"encoding/csv"
	"io"
	"os"
)

func main() {
	reader := csv.NewReader(os.Stdin)
	reader.FieldsPerRecord = -1 // just in case variable fields

	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		var output []string
		if len(line) > 3 {
			output = line[:3]
		} else {
			output = line
		}

		_ = writer.Write(output)
	}
}
