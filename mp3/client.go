package main

import (
	"bytes"
	"errors"
	"fmt"
	"g14-mp3/failureDetector"
	"g14-mp3/mp3/resources"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Client struct {
	stdin              chan string
	server             *Server
	myNode             failureDetector.NodeId
	appendNumber       int
	appendNumbersMutex sync.Mutex
}

const (
	createRpc string = "CreateFile"
	appendRpc string = "Append"
)

func (c *Client) client() {
	c.appendNumber = 0

	//listener for multi append
	go func() {
		rpcListener, err := net.Listen("tcp", ":8011")
		if err != nil {
			fmt.Println(err)
		}
		err = rpc.Register(c)
		if err != nil {
			fmt.Println(err)
		}
		rpc.Accept(rpcListener)
	}()

	for {
		input := <-c.stdin
		fields := strings.Fields(input)
		if len(fields) < 1 {
			continue
		}
		command := fields[0]
		var args []string
		if len(fields) > 1 {
			args = fields[1:]
		}
		switch command {
		case "create":
			if len(args) != 2 {
				fmt.Println("Incorrect len of args for create")
				break
			}
			err := c.addContentAtNodes(args[0], args[1], 3, createRpc)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Client: Create completed")
			}
			break
		case "append":
			if len(args) != 2 {
				fmt.Println("Incorrect len of args for append")
				break
			}
			err := c.addContentAtNodes(args[0], args[1], 2, appendRpc)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Client: Append completed")
			}
			break
		case "get":
			if len(args) != 2 {
				fmt.Println("Incorrect len of args for get")
				break
			}
			err := c.getFile(args[0], args[1])
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Client: Get completed")
			}
			break
		case "merge":
			if len(args) != 1 {
				fmt.Println("Incorrect len of args for merge")
				break
			}
			err := c.merge(args[0])
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Client: Force Merge Completed")
			}
			break
		case "list_mem_ids":
			nodeIds := c.server.nodesList()
			for _, n := range nodeIds {
				fmt.Printf("%d : %s\n", n.Hash, n.NodeId.String())
			}
			break
		case "ls":
			if len(args) != 1 {
				fmt.Println("Incorrect len of args for ls")
				break
			}
			nodes, err := c.getReplicaNodes(args[0])
			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println(fmt.Sprintf("FileID: %d", resources.HashString(args[0])))
			for n := range nodes {
				fmt.Println(fmt.Sprintf("Replica %d: ", n+1) + nodes[n].String())
			}
			break
		case "multiappend":
			if len(args)%2 == 0 {
				fmt.Println("Incorrect len of args for multiappend")
				break
			}
			hd, err := os.UserHomeDir()
			if err != nil {
				fmt.Println(err)
				break
			}
			servers, err := os.ReadFile(filepath.Join(hd, "g14-mp3", "mp3", "resources", "servers.conf"))
			if err != nil {
				servers, err = os.ReadFile(filepath.Join("mp3", "resources", "servers.conf"))
				if err != nil {
					fmt.Println(err)
					break
				}
			}
			splitServers := bytes.Split(servers, []byte("\n"))
			numVms := len(args) / 2
			waitingChan := make(chan *rpc.Call, numVms)
			rpcServers := make([]*rpc.Client, numVms)
			for i := 0; i < numVms; i++ {
				vmNum, _ := strconv.Atoi(args[i+1])
				vmFile := args[i+numVms+1]
				serverIP := splitServers[vmNum-1]
				rpcServers[i], err = rpc.Dial("tcp", string(serverIP)+":8011")
				if err != nil {
					fmt.Println(err)
					waitingChan <- &rpc.Call{} // fill with dummy values for unsuccessful vms
					continue
				}
				var reply int
				rpcServers[i].Go("Client.MultiAppend", resources.MultiAppendArgs{LocalName: vmFile, RemoteName: args[0]}, &reply, waitingChan)
			}
			for i := 0; i < numVms; i++ {
				<-waitingChan
			}
			for i := 0; i < numVms; i++ {
				if rpcServers[i] != nil {
					_ = rpcServers[i].Close()
				}
			}
			fmt.Println("Client: Multiappend completed")
			break
		case "getfromreplica":
			if len(args) != 3 {
				fmt.Println("Incorrect len of args for getfromreplica")
				break
			}

			server, err := rpc.Dial("tcp", args[0]+":8010")
			if err != nil {
				fmt.Println(err)
				break
			}
			var reply []byte
			err = server.Call("Server.GetFromReplica", &args[1], &reply)
			if err != nil {
				fmt.Println(&resources.FileNotFoundError{FileName: args[1]})
				break
			}
			file, err := os.Create(args[2])
			if err != nil {
				fmt.Println(err)
				break
			}

			_, err = file.Write(reply)
			if err != nil {
				fmt.Println(err)
				break
			}
			_ = file.Close()
			fmt.Println("Client: Get from replica completed")
			break
		}
	}
}

func (c *Client) addContentAtNodes(localName string, remoteName string, numNodesWanted int, addType string) error {
	if _, err := os.Stat(localName); err == nil {
		// file exists locally
		c.appendNumbersMutex.Lock()
		defer c.appendNumbersMutex.Unlock()
		nodes, err := c.getReplicaNodes(remoteName)
		if err != nil {
			return err
		}
		localFileContent, _ := os.ReadFile(localName)
		serversCalled := 0
		waitChan := make(chan *rpc.Call, numNodesWanted)
		replies := make([]resources.AddFileReply, numNodesWanted)
		randNum := rand.Intn(len(nodes))
		for i := 0; i < numNodesWanted; i++ {
			curNode := nodes[(randNum+i)%len(nodes)]
			args := resources.AddFileArgs{
				HDFSFileName: remoteName,
				Content:      localFileContent,
				AppendNumber: resources.AppendNumber{
					NodeId:  c.myNode,
					Counter: c.appendNumber,
				},
			}

			curServer, err := rpc.Dial("tcp", curNode.IP()+":8010")
			if err == nil {
				curServer.Go(fmt.Sprintf("Server.%s", addType), &args, &replies[i], waitChan)
				serversCalled++
			}
		}

		for i := 0; i < serversCalled; i++ {
			l := <-waitChan
			err := l.Reply.(*resources.AddFileReply).Err
			if err != nil {
				//error
				var existsErr *resources.FileAlreadyExistsError
				var notFoundErr *resources.FileNotFoundError
				switch {
				case errors.As(err, &notFoundErr) || errors.As(err, &existsErr):
					fmt.Println(err)
					break
				default:
					fmt.Println("unexpected error:", err)
				}
			}
		}
		c.appendNumber++

		return nil
	} else {
		//file does not exist locally
		return err
	}
}

func (c *Client) getFile(remoteFile, localFile string) error {
	nodes, err := c.getReplicaNodes(remoteFile)
	if err != nil {
		return err
	}
	randReplicaIdx := rand.Intn(len(nodes))
	replicaNode := nodes[randReplicaIdx]
	var reply []byte
	curServer, err := rpc.Dial("tcp", replicaNode.IP()+":8010")
	if err != nil {
		return err
	}
	err = curServer.Call("Server.Get", &remoteFile, &reply)
	if err != nil {
		return &resources.FileNotFoundError{FileName: remoteFile}
	}
	file, err := os.Create(localFile)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	_, err = file.Write(reply)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) merge(remoteFile string) error {

	nodes, err := c.getReplicaNodes(remoteFile)
	primaryNode := nodes[0]
	if err != nil {
		return err
	}
	server, err := rpc.Dial("tcp", primaryNode.IP()+":8010")
	if err != nil {
		return err
	}
	var reply int
	err = server.Call("Server.MergeFile", &remoteFile, &reply)
	if err != nil {
		return err
	}
	_ = server.Close()
	return nil
}

func (c *Client) getReplicaNodes(remoteFile string) ([]failureDetector.NodeId, error) {
	coordinatorAddr := c.myNode.IP() + ":8010"

	server, err := rpc.Dial("tcp", coordinatorAddr)
	if err != nil {
		return nil, fmt.Errorf("error connecting to server %s: %w", coordinatorAddr, err)
	}

	args := resources.CoordinatorArgs{HDFSFileName: remoteFile}
	var coordReply resources.CoordinatorReply
	//Call the coordinator to ask which 3 nodes the file should be created on
	err = server.Call("Server.RequestCoordinator", &args, &coordReply)
	if err != nil {
		return nil, err
	}
	err = server.Close()
	if err != nil {
		return nil, fmt.Errorf("error closing server %s: %w", coordinatorAddr, err)
	}
	return coordReply.Nodes, nil
}

func (c *Client) MultiAppend(args resources.MultiAppendArgs, reply *int) error {
	err := c.addContentAtNodes(args.LocalName, args.RemoteName, 2, appendRpc)
	if err != nil {
		return err
	}
	return nil
}
