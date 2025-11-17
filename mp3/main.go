package main

import (
	"bufio"
	"os"
)

func main() {
	server := Server{stdin: make(chan string)}
	server.init()
	go server.server()
	client := Client{stdin: make(chan string), server: &server, myNode: server.mySelf()}
	go client.client()
	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		input := scanner.Text()
		client.stdin <- input
		server.stdin <- input
	}
}
