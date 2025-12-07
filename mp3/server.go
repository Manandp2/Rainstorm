package main

import (
	"bytes"
	"cmp"
	"container/list"
	"errors"
	"fmt"
	"g14-mp4/failureDetector"
	"g14-mp4/mp3/resources"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

type HashedNode struct {
	NodeId failureDetector.NodeId
	Hash   uint32
}

const bufferSize = 1 << 20 // 1 MB
const folderName = "HyDFS"
const mergeTime = 1 * time.Minute

type Server struct {
	stdin           chan string
	detector        failureDetector.Detector
	hashedNodesLock sync.RWMutex
	hashedNodes     []HashedNode
	files           map[string]*localFile
	filesLock       sync.RWMutex
}

type localFile struct {
	file resources.File
	lock sync.RWMutex
}

func (s *Server) init() {
	s.files = make(map[string]*localFile)

	// set up RPC
	err := rpc.Register(s)
	if err != nil {
		return
	}
	go func() {
		listener, err := net.Listen("tcp", ":8010")
		if err != nil {
			return
		}
		rpc.Accept(listener)
	}()

	// Set up the folder to store files
	_ = os.RemoveAll(folderName)
	_ = os.Mkdir(folderName, 0777)

	// Get membership list setup
	s.detector.Init()
}

func (s *Server) server() {
	go s.detector.Start()
	go func() {
		for {
			select {
			case n := <-s.detector.AddMe:
				curHashed := resources.HashNode(n)
				s.hashedNodesLock.Lock()
				insertIdx, exists := slices.BinarySearchFunc(s.hashedNodes, curHashed, compareHashedNode)
				if !exists {
					s.hashedNodes = append(s.hashedNodes, HashedNode{})
					copy(s.hashedNodes[insertIdx+1:], s.hashedNodes[insertIdx:])
					s.hashedNodes[insertIdx] = HashedNode{
						NodeId: n,
						Hash:   curHashed,
					}
				}
				// When a node joins, check all files to see if you were removed from that files nodes
				// 	if you were, send your file contents to that node, and delete it from yourself
				waitingChan := make(chan *rpc.Call, len(s.files))
				counter := 0
				s.filesLock.Lock()
				for fileName, file := range s.files {
					h := resources.HashString(fileName)
					i, _ := slices.BinarySearchFunc(s.hashedNodes, h, compareHashedNode)
					if s.hashedNodes[(i+3)%len(s.hashedNodes)].NodeId == s.detector.MyNode {
						//we were previously successor 2, the new node got added and kicked us out
						//send our file to the new node and delete from ourselves
						file.lock.Lock()
						diskContent, _ := os.ReadFile(filepath.Join(folderName, fileName))
						args := resources.SendFileArgs{
							FileName:      fileName,
							DiskContent:   diskContent,
							BufferContent: listToSlice[resources.AppendBlock](file.file.AppendBuffer),
						}
						newNode, err := rpc.Dial("tcp", n.IP()+":8010")
						if err != nil {
							continue
						}
						var reply int
						newNode.Go("Server.SendFile", args, &reply, waitingChan)
						counter++
						file.lock.Unlock()
						delete(s.files, fileName)
					}
				}
				s.hashedNodesLock.Unlock()
				s.filesLock.Unlock()
				for i := 0; i < counter; i++ {
					l := <-waitingChan
					if l.Error != nil {
						fmt.Println(l.Error)
					}
				}
			case n := <-s.detector.RemoveMe:
				// Go through all files
				// 	- for each file that contains the removed node,
				//		check if you are (primary) or (successor1 iff primary is the removed node)
				//			send info to NEW successor2
				s.hashedNodesLock.Lock()
				s.filesLock.RLock()
				waitingChan := make(chan *rpc.Call, len(s.files))
				counter := 0
				for fileName, file := range s.files {
					h := resources.HashString(fileName)
					i, _ := slices.BinarySearchFunc(s.hashedNodes, h, compareHashedNode)
					if s.hashedNodes[(i)%len(s.hashedNodes)].NodeId == n ||
						s.hashedNodes[(i+1)%len(s.hashedNodes)].NodeId == n ||
						s.hashedNodes[(i+2)%len(s.hashedNodes)].NodeId == n {
						//	file is going to be impacted by the remove
						if s.hashedNodes[(i)%len(s.hashedNodes)].NodeId == s.detector.MyNode ||
							(s.hashedNodes[(i)%len(s.hashedNodes)].NodeId == n &&
								s.hashedNodes[(i+1)%len(s.hashedNodes)].NodeId == s.detector.MyNode) {
							diskContent, _ := os.ReadFile(filepath.Join(folderName, fileName))
							args := resources.SendFileArgs{
								FileName:      fileName,
								DiskContent:   diskContent,
								BufferContent: listToSlice[resources.AppendBlock](file.file.AppendBuffer),
							}
							newNode, err := rpc.Dial("tcp", s.hashedNodes[(i+3)%len(s.hashedNodes)].NodeId.IP()+":8010")
							if err != nil {
								fmt.Println(err)
								continue
							}
							var reply int
							newNode.Go("Server.SendFile", args, &reply, waitingChan)
							counter++
						}
					}
				}
				s.filesLock.RUnlock()
				for i := 0; i < counter; i++ {
					l := <-waitingChan
					if l.Error != nil {
						fmt.Println(l.Error)
					}
				}
				curHashed := resources.HashNode(n)
				removalIdx, exists := slices.BinarySearchFunc(s.hashedNodes, curHashed, compareHashedNode)
				if exists {
					s.hashedNodes = append(s.hashedNodes[:removalIdx], s.hashedNodes[removalIdx+1:]...)
				}
				s.hashedNodesLock.Unlock()
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(mergeTime)
		for {
			<-ticker.C
			s.backgroundMerge()
		}
	}()

	for {
		switch input := <-s.stdin; input {
		case "liststore":
			s.listStore()
			break
		default:
			s.detector.ProcessInput(input)
		}

	}
}
func (s *Server) SendFile(args resources.SendFileArgs, reply *int) error {
	s.filesLock.Lock()
	s.files[args.FileName] = &localFile{
		file: resources.File{
			Name:         args.FileName,
			AppendBuffer: sliceToList[resources.AppendBlock](args.BufferContent),
		},
		lock: sync.RWMutex{},
	}
	s.filesLock.Unlock()
	_ = os.WriteFile(filepath.Join(folderName, args.FileName), args.DiskContent, 0666)
	return nil
}

// RequestCoordinator is sent to the coordinator from a client to create a file.
// The coordinator will then send the request to the 3 nodes that the file hashes to.
func (s *Server) RequestCoordinator(args *resources.CoordinatorArgs, reply *resources.CoordinatorReply) error {
	fileHash := resources.HashString(args.HDFSFileName)

	s.hashedNodesLock.RLock()
	i, _ := slices.BinarySearchFunc(s.hashedNodes, fileHash, compareHashedNode)
	reply.Nodes = make([]failureDetector.NodeId, 3)
	for j := range reply.Nodes {
		reply.Nodes[j] = s.hashedNodes[(i+j)%len(s.hashedNodes)].NodeId
	}
	s.hashedNodesLock.RUnlock()

	return nil
}

func (s *Server) CreateFile(args *resources.AddFileArgs, reply *resources.AddFileReply) error {
	s.filesLock.Lock()
	_, exists := s.files[args.HDFSFileName]
	if exists {
		s.filesLock.Unlock()
		reply.Err = &resources.FileAlreadyExistsError{FileName: args.HDFSFileName}
		fmt.Println("Sever: Error CreateFile " + reply.Err.Error())
		return nil
	}

	s.files[args.HDFSFileName] = &localFile{
		file: resources.File{
			Name:         args.HDFSFileName,
			AppendBuffer: list.New(),
		},
		lock: sync.RWMutex{},
	}
	f := s.files[args.HDFSFileName]
	f.lock.Lock()
	fmt.Println("Server: Start CreateFile " + args.HDFSFileName)
	defer f.lock.Unlock()

	appendBuffer := f.file.AppendBuffer
	s.filesLock.Unlock()

	reader := bytes.NewReader(args.Content)
	for {
		e := resources.AppendBlock{
			AppendNumber: args.AppendNumber,
			Data:         make([]byte, bufferSize),
		}
		numRead, _ := reader.Read(e.Data)
		if numRead == 0 {
			break
		}
		e.Data = e.Data[:numRead]
		appendBuffer.PushBack(e)
	}
	fmt.Println("Server: Finish CreateFile " + args.HDFSFileName)
	return nil
}

func (s *Server) Append(args *resources.AppendArgs, reply *resources.AppendReply) error {
	s.filesLock.RLock()
	f, exists := s.files[args.HDFSFileName]
	s.filesLock.RUnlock()
	if !exists {
		reply.Err = &resources.FileNotFoundError{FileName: args.HDFSFileName}
		fmt.Println("Sever: Error Append " + reply.Err.Error())
		return nil
	}
	f.lock.Lock()
	fmt.Println("Server: Start Append " + args.HDFSFileName)
	defer f.lock.Unlock()
	file := &f.file
	// find where to insert, order is first based on the NodeId, and then within a NodeId, by appendNumber
	// predecessor will be the element we insert AFTER.
	var predecessor *list.Element = nil

	for e := file.AppendBuffer.Front(); e != nil; e = e.Next() {
		currentItem := e.Value.(resources.AppendBlock).AppendNumber
		comparison := compareForInsertion(args.AppendNumber, currentItem)

		if comparison == -1 {
			predecessor = e.Prev()
			break
		} else {
			predecessor = e
		}
	}

	reader := bytes.NewReader(args.Content)
	for {
		e := resources.AppendBlock{
			AppendNumber: args.AppendNumber,
			Data:         make([]byte, bufferSize),
		}
		numRead, _ := reader.Read(e.Data)
		if numRead == 0 {
			break
		}
		e.Data = e.Data[:numRead]

		if predecessor == nil {
			predecessor = file.AppendBuffer.PushFront(e)
		} else {
			predecessor = file.AppendBuffer.InsertAfter(e, predecessor)
		}
	}
	fmt.Println("Server: Finish Append " + args.HDFSFileName)
	return nil
}

func (s *Server) Get(fileName string, reply *[]byte) error {
	// client asks a random replica(us)
	// we ask one of the other replicas for their linked list
	// combine their linked list with our linked list (nothing to do with background merging)
	// add the combined linked list to the end of the byte stream
	// we give the client the byte stream
	// all good!
	s.filesLock.RLock()
	fmt.Println("Server: Start Get " + fileName)
	f, ok := s.files[fileName]
	s.filesLock.RUnlock()
	if !ok {
		fmt.Println("Sever: Error Get " + (&resources.FileNotFoundError{FileName: fileName}).Error())
		return errors.New("file not found")
	}

	args := resources.CoordinatorArgs{HDFSFileName: fileName}
	var replicas resources.CoordinatorReply
	_ = s.RequestCoordinator(&args, &replicas) // err is always nil
	var replica failureDetector.NodeId
	var remoteList []resources.AppendBlock
	for {
		randReplicaIdx := rand.Intn(len(replicas.Nodes))
		if replicas.Nodes[randReplicaIdx] != s.detector.MyNode {
			replica = replicas.Nodes[randReplicaIdx]
			client, err := rpc.Dial("tcp", replica.IP()+":8010")
			if err != nil {
				return err
			}
			err = client.Call("Server.GetList", fileName, &remoteList)
			if err != nil {
				continue
			}
			_ = client.Close()
			break
		}
	}

	f.lock.RLock()

	// merge the append buffers
	localList := f.file.AppendBuffer
	remoteLinkedList := sliceToList[resources.AppendBlock](remoteList)
	local := localList.Front()
	remote := remoteLinkedList.Front()
	mergedList := getMergedList(local, remote)
	f.lock.RUnlock()
	// get the local file in HyDFS
	*reply, _ = os.ReadFile(filepath.Join(folderName, fileName))

	//add the things in the combined buffer to the file content that we are sending back,
	//this will eventually occur on the server as well when a background merge occurs
	for e := mergedList.Front(); e != nil; e = e.Next() {
		*reply = append(*reply, e.Value.(resources.AppendBlock).Data...)
	}
	fmt.Println("Sever: Finish Get " + fileName)
	return nil
}

func (s *Server) GetFromReplica(fileName string, reply *[]byte) error {
	s.filesLock.RLock()
	fmt.Println("Server: Start GetFromReplica " + fileName)
	f, ok := s.files[fileName]
	s.filesLock.RUnlock()
	if !ok {
		fmt.Println("Sever: Error GetFromReplica " + (&resources.FileNotFoundError{FileName: fileName}).Error())
		return errors.New("file not found")
	}
	f.lock.RLock()
	*reply, _ = os.ReadFile(filepath.Join(folderName, fileName))
	for e := f.file.AppendBuffer.Front(); e != nil; e = e.Next() {
		*reply = append(*reply, e.Value.(resources.AppendBlock).Data...)
	}
	f.lock.RUnlock()
	fmt.Println("Sever: Finish GetFromReplica " + fileName)
	return nil
}

func (s *Server) backgroundMerge() {
	s.filesLock.RLock()
	s.hashedNodesLock.RLock()
	for fileName := range s.files {
		var t int
		_ = s.MergeFile(fileName, &t)
	}
	s.hashedNodesLock.RUnlock()
	s.filesLock.RUnlock()
}

func (s *Server) merge(remoteFile *resources.ExportedFile) *list.List {
	if remoteFile == nil {
		return nil
	}
	fileName := remoteFile.Name
	// read lock on this file when reading it
	s.files[fileName].lock.Lock()
	fmt.Println("Server: Start merge")
	local := s.files[fileName].file.AppendBuffer.Front()
	remoteAppendBuffer := sliceToList[resources.AppendBlock](remoteFile.AppendBuffer)
	remote := remoteAppendBuffer.Front()
	mergedList := getMergedList(local, remote)

	// clear the Append buffer for the file locally
	s.files[fileName].file.AppendBuffer.Init()
	s.files[fileName].lock.Unlock()
	//write the merged list to disk
	s.writeToDisk(remoteFile.Name, mergedList)
	fmt.Println("Server: Finish merge")
	return mergedList
}

func (s *Server) MergeGivenList(remoteFile *resources.ExportedFile, reply *int) error {
	//this function only impacts successor 2
	s.filesLock.RLock()
	fmt.Println("Server: Start merge")
	f, ok := s.files[remoteFile.Name]
	s.filesLock.RUnlock()
	if !ok {
		return nil
	}
	local := f.file.AppendBuffer.Front()
	remoteAppendBuffer := sliceToList[resources.AppendBlock](remoteFile.AppendBuffer)
	remote := remoteAppendBuffer.Front()
	buffer := list.New()
	f.lock.Lock()
	for remote != nil && local != nil {
		cmpVal := compareForInsertion(remote.Value.(resources.AppendBlock).AppendNumber, local.Value.(resources.AppendBlock).AppendNumber)
		if cmpVal == 0 {
			//local and remote point to the same node add to buffer and both move on. since added a local node, remove it from the local buffer list
			t := local.Next()
			buffer.PushBack(f.file.AppendBuffer.Remove(local).(resources.AppendBlock))
			local = t
			remote = remote.Next()
		} else if cmpVal == -1 {
			//skip ahead in remote because local did not receive this append block
			buffer.PushBack(remote.Value.(resources.AppendBlock))
			remote = remote.Next()
		} else if cmpVal == 1 {
			if resources.CompareNodeId(remote.Value.(resources.AppendBlock).AppendNumber.NodeId, local.Value.(resources.AppendBlock).AppendNumber.NodeId) == 1 {
				local = local.Next()
			} else {
				//should never be hit
				//fmt.Println("Local Append buffer:")
				//for e := f.file.AppendBuffer.Front(); e != nil; e = e.Next() {
				//	fmt.Printf("%+v , \n", e.Value.(resources.AppendBlock).AppendNumber)
				//}
				//fmt.Println("Remote Append buffer:")
				//for _, a := range remoteFile.AppendBuffer {
				//	fmt.Printf("%+v , \n", a.AppendNumber)
				//}
				//panic("there was an append missing from primary and successor1, quorum consistency was broken")
			}
		}
	}

	//remote still has AppendBlocks that local did not receive
	for ; remote != nil; remote = remote.Next() {
		buffer.PushBack(remote.Value.(resources.AppendBlock))
	}

	//Need to clear because no nodes were received after the merge occurred on primary and successor 1
	if local == nil {
		f.file.AppendBuffer.Init()
	}
	f.lock.Unlock()
	s.writeToDisk(remoteFile.Name, buffer)
	fmt.Println("Server: Finish merge")
	return nil
}

func (s *Server) GetList(fileName string, reply *[]resources.AppendBlock) error {
	s.filesLock.RLock()
	defer s.filesLock.RUnlock()
	s.files[fileName].lock.RLock()
	defer s.files[fileName].lock.RUnlock()
	*reply = listToSlice[resources.AppendBlock](s.files[fileName].file.AppendBuffer)
	return nil
}

func (s *Server) nodesList() []HashedNode {
	s.hashedNodesLock.RLock()
	defer s.hashedNodesLock.RUnlock()
	return s.hashedNodes
}
func (s *Server) ExchangeBuffers(args, reply *resources.ExportedFile) error {
	if _, ok := s.files[args.Name]; !ok {
		return nil
	}
	s.files[args.Name].lock.RLock()
	*reply = resources.ExportedFile{
		Name:         args.Name,
		AppendBuffer: listToSlice[resources.AppendBlock](s.files[args.Name].file.AppendBuffer),
	}
	s.files[args.Name].lock.RUnlock()
	go s.merge(args)
	return nil
}
func (s *Server) mySelf() failureDetector.NodeId {
	return s.detector.MyNode
}

func (s *Server) writeToDisk(fileName string, updatedList *list.List) {
	f, _ := os.OpenFile(filepath.Join(folderName, fileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	for e := updatedList.Front(); e != nil; e = e.Next() {
		_, _ = f.Write(e.Value.(resources.AppendBlock).Data)
	}
}

func compareHashedNode(node HashedNode, u uint32) int {
	return cmp.Compare(node.Hash, u)
}

// compareForInsertion compares two AppendNumber instances for insertion order.
// It first compares by NodeId and then by Counter if NodeIds are equal.
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
func compareForInsertion(a, b resources.AppendNumber) int {
	// 1. Compare by NodeId first
	if c := resources.CompareNodeId(a.NodeId, b.NodeId); c != 0 {
		return c
	}
	// 2. NodeIds are equal, so we compare by Counter
	return cmp.Compare(a.Counter, b.Counter)
}

func listToSlice[T any](l *list.List) []T {
	if l == nil {
		return nil
	}
	slice := make([]T, 0, l.Len())
	for e := l.Front(); e != nil; e = e.Next() {
		slice = append(slice, e.Value.(T))
	}
	return slice
}

// sliceToList converts a slice of type []T to a *list.List.
func sliceToList[T any](slice []T) *list.List {
	l := list.New()
	for _, v := range slice {
		l.PushBack(v)
	}
	return l
}

// getMergedList merges two ordered linked lists of type resources.AppendBlock into a single ordered linked list.
// The function assumes both input lists are sorted and compares their elements using compareForInsertion and that file is locked.
// It adds remaining elements from any non-exhausted list to the merged list after one list is fully traversed.
// Returns a newly created *list.List that contains the merged and sorted elements from both input lists.
func getMergedList(local, remote *list.Element) *list.List {
	//go through both lists and merge until you reach the end of one of the lists
	mergedList := list.New()
	for local != nil && remote != nil {
		localVal := local.Value.(resources.AppendBlock)
		remoteVal := remote.Value.(resources.AppendBlock)
		switch compareForInsertion(localVal.AppendNumber, remoteVal.AppendNumber) {
		case -1:
			//local should come before the remote
			mergedList.PushBack(localVal)
			local = local.Next()
			break
		case 1:
			//remote should come before the block at the local pointer
			mergedList.PushBack(remoteVal)
			remote = remote.Next()
			break
		case 0:
			// the same node, add one and move both forward
			mergedList.PushBack(localVal)
			local = local.Next()
			remote = remote.Next()
			break
		}
	}

	//either local or remote finished, need to append the rest of the non-nil one
	for ; local != nil; local = local.Next() {
		localVal := local.Value.(resources.AppendBlock)
		mergedList.PushBack(localVal)
	}
	for ; remote != nil; remote = remote.Next() {
		remoteVal := remote.Value.(resources.AppendBlock)
		mergedList.PushBack(remoteVal)
	}

	return mergedList
}

func (s *Server) MergeFile(fileName string, reply1 *int) error {
	fileHash := resources.HashString(fileName)
	i, _ := slices.BinarySearchFunc(s.hashedNodes, fileHash, compareHashedNode)
	if s.hashedNodes[i%len(s.hashedNodes)].NodeId == s.detector.MyNode {
		successor1, successor2 := s.hashedNodes[(i+1)%len(s.hashedNodes)].NodeId, s.hashedNodes[(i+2)%len(s.hashedNodes)].NodeId

		// is the primary --> need to initiate the merge
		/*
			merge with successor 1
			write to disk
			send to successor 2
		*/
		serverSuccessor1, err := rpc.Dial("tcp", successor1.IP()+":8010")
		if err != nil {
			//successor 1 went down
			return err
		}
		var successor1ExpFile resources.ExportedFile
		// merge and clear successor 1
		s.files[fileName].lock.RLock()
		primaryExpFile := resources.ExportedFile{
			Name:         fileName,
			AppendBuffer: listToSlice[resources.AppendBlock](s.files[fileName].file.AppendBuffer),
		}
		s.files[fileName].lock.RUnlock()
		err = serverSuccessor1.Call("Server.ExchangeBuffers", &primaryExpFile, &successor1ExpFile)
		if err != nil {
			// successor 1 went down
			return err
		}
		_ = serverSuccessor1.Close()
		// merge and clear primary
		mergedList := s.merge(&successor1ExpFile)
		if mergedList == nil {
			s.files[fileName].lock.RLock()
			mergedList = s.files[fileName].file.AppendBuffer
			s.files[fileName].lock.RUnlock()
		}
		tempExpFile := resources.ExportedFile{
			Name:         fileName,
			AppendBuffer: listToSlice[resources.AppendBlock](mergedList),
		}
		serverSuccessor2, err := rpc.Dial("tcp", successor2.IP()+":8010")
		if err != nil {
			//successor 2 went down
			return err
		}
		var reply int
		// merge and clear successor 2
		err = serverSuccessor2.Call("Server.MergeGivenList", &tempExpFile, &reply)
		if err != nil {
			//successor 2 down
			return err
		}
		_ = serverSuccessor2.Close()
	}
	return nil
}

func (s *Server) listStore() {
	s.filesLock.RLock()
	defer s.filesLock.RUnlock()
	for fileName := range s.files {
		fmt.Println(fileName)
	}
}
