package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	pattern := os.Args[1]
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		if strings.Contains(text, pattern) {
			fmt.Println(scanner.Text() + "\n")
		}
	}
}
