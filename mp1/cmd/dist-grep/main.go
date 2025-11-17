package main

import (
	"fmt"
	. "g14-mp3/mp1/resources/rpc-api"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	serverAddresses, err := readServerAddresses()
	if err != nil {
		fmt.Println(err)
		return
	}
	replyChan := make(chan *rpc.Call, len(serverAddresses))
	numServersSuccess := 0
	if serverAddresses[len(serverAddresses)-1] == "" {
		serverAddresses = serverAddresses[:len(serverAddresses)-1]
	}
	for _, addr := range serverAddresses {
		server, err := processMachine(addr, &replyChan)
		if err != nil {
			fmt.Println(err)
			continue
		}
		numServersSuccess++
		//goland:noinspection GoDeferInLoop
		defer func() {
			_ = server.Close()
		}()
	}
	// Go through all responses in replyChan (order is not needed)
	totalNumberOfMatches := 0
	var counts strings.Builder
	for i := 0; i < numServersSuccess; i++ {
		call := <-replyChan
		reply := call.Reply.(*GrepReply)
		if call.Error != nil {
			fmt.Println(call.Error)
		} else {
			fmt.Printf("Results from vm%s:\n", reply.VMNumber)
			totalNumberOfMatches += reply.LineCount
			_, _ = os.Stdout.Write(reply.Output)
			_ = os.WriteFile(fmt.Sprintf("results/vm_%s_grep_result", reply.VMNumber), reply.Output, 0644)
			counts.WriteString(fmt.Sprintf("Number of Matches from VM%s: %d\n", reply.VMNumber, reply.LineCount))
		}
	}
	fmt.Print(counts.String())
	fmt.Printf("Total number of matching lines summed across all VMs: %d\n", totalNumberOfMatches)
}

func readServerAddresses() ([]string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	filePath := filepath.Join(wd, "servers.conf")
	serverBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("no servers.conf found: %w", err)
	}
	return strings.Split(string(serverBytes), "\n"), nil
}

func processMachine(addr string, replyChan *chan *rpc.Call) (*rpc.Client, error) {
	// connecting to each server
	server, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to server %s: %w", addr, err)
	}
	// calling each server's Grep method
	vmNumber := addr[13:15]
	if vmNumber[0] == '0' {
		vmNumber = string(vmNumber[1])
	}
	args := GrepArgs{VMNumber: vmNumber, Args: os.Args[1:]}

	var reply GrepReply
	// replyChan receives this call when the function completes
	server.Go("GrepService.Grep", &args, &reply, *replyChan)
	return server, nil
}
