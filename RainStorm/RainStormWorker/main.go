package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	. "g14-mp4/RainStorm/resources"
	"g14-mp4/mp3/resources"
	"io"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type localTask struct {
	cmd            *exec.Cmd
	input          io.WriteCloser // To send tuples to tasks (receives data from tcp)
	output         io.ReadCloser  // To send tuples to the next stage (sends data through tcp)
	inputRate      int
	startTime      time.Time
	lastCheckTime  time.Time
	lastInputCount int
	logFile        *os.File
}

type taskOutput struct {
	tupleId int
	taskId  TaskID
	output  string
}

type logRequest struct {
	fileName string
	data     string
}

type Worker struct {
	rainStormLeader    *rpc.Client // used to send task completions
	rainStormStartTime string
	hydfsClient        *rpc.Client
	hydfsDestFile      string
	lowWatermark       float64
	highWatermark      float64

	done        chan bool
	tasksLocker sync.RWMutex
	tasks       map[TaskID]*localTask

	ips           []map[int]*TaskInfo // ips of machines with [stage][task] indexing
	taskIDLocker  sync.RWMutex
	sortedTaskIDs [][]int // used to find the task # within a given stage
	logChan       chan logRequest

	taskOutputs     chan taskOutput
	connections     map[string]*WorkerClient
	connectionsLock sync.RWMutex
	stageOperations []Operation

	tuplesLock     sync.Mutex
	receivedTuples map[string]bool // key = taskId-TupleId, value is dummy
	tupleSendConn  net.Conn
}

const clientTimeout = time.Second * 3
const ACK = "ACK"

func main() {
	leader, err := rpc.Dial("tcp", "fa25-cs425-1401.cs.illinois.edu"+IntroducePort)
	if err != nil {
		fmt.Println(err)
		return
	}
	var reply int
	err = leader.Call("WorkerIps.AddWorker", getOutboundIP(), &reply)
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = leader.Close()
	hydfsClient, err := rpc.Dial("tcp", "localhost:8011") // connect to our own HydFS client
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		server := rpc.NewServer()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue // try again
		}
		worker := Worker{
			hydfsClient:    hydfsClient,
			done:           make(chan bool),
			tasks:          make(map[TaskID]*localTask),
			taskOutputs:    make(chan taskOutput, 100),
			connections:    make(map[string]*WorkerClient),
			receivedTuples: make(map[string]bool),
			logChan:        make(chan logRequest, 1000),
		}
		err = server.Register(&worker)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		leaderListener, err := net.Listen("tcp", AssignmentPort)
		if err != nil {
			//fmt.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}
		go server.Accept(leaderListener)

		// Goroutine for sending out tuples
		go func() {
			for {
				// On output of tuple from a task, send it to the next task
				out := <-worker.taskOutputs
				nextStage := out.taskId.Stage + 1
				// Remote log
				worker.logChan <- logRequest{
					fileName: fmt.Sprintf("%s_%d-%d", worker.rainStormStartTime, out.taskId.Stage, out.taskId.Task),
					data:     fmt.Sprintf("PROCESSED,%s-%d,%s\n", out.taskId.String(), out.tupleId, out.output),
				}

				// local log
				worker.tasksLocker.RLock()
				if t, ok := worker.tasks[out.taskId]; ok && t.logFile != nil {
					_, _ = fmt.Fprintln(t.logFile, "OUTPUT: ", out.output)
				}
				worker.tasksLocker.RUnlock()
				if nextStage < len(worker.ips) { // send it to the next stage
					key := out.output
					if worker.stageOperations[nextStage].Name == AggregateByKey {
						hashIndex, err := strconv.Atoi(worker.stageOperations[nextStage].Args)
						if err != nil {
							hashIndex = 0
						}
						reader := csv.NewReader(strings.NewReader(out.output))
						tuple, err := reader.Read()
						if err == nil && hashIndex < len(tuple) {
							key = tuple[hashIndex]
						}
					}

					// Find which client gets the next tuple
					worker.taskIDLocker.RLock()
					nextStageTasks := worker.sortedTaskIDs[nextStage]
					hash := HashString(key)
					if hash < 0 { // make sure hash is positive
						hash = -hash
					}
					nextTask := nextStageTasks[hash%len(nextStageTasks)] // Go to the sorted array and find the task #

					nextWorker := worker.ips[nextStage][nextTask].Ip.String()
					worker.taskIDLocker.RUnlock()

					worker.connectionsLock.RLock()
					client, ok := worker.connections[nextWorker]
					worker.connectionsLock.RUnlock()
					if !ok { // new connection,
						// try connecting
						conn, err := net.Dial("tcp", nextWorker+TuplePort)
						if err != nil {
							worker.taskOutputs <- out
							continue
						}
						newClient := &WorkerClient{
							Conn: conn,
							Buf:  bufio.NewReader(conn),
						}

						worker.connectionsLock.Lock()
						// Make sure the client wasn't already added while we were dialing
						if existing, exists := worker.connections[nextWorker]; exists {
							// Already exists, just use that one
							client = existing
							conn.Close()
						} else {
							client = newClient
							worker.connections[nextWorker] = client
						}
						worker.connectionsLock.Unlock()
					}

					// Send the tuple
					_ = client.Conn.SetWriteDeadline(time.Now().Add(clientTimeout))
					// Id-Id, stage, task, data
					_, err = fmt.Fprintf(client.Conn, "%s-%d,%d,%d,%s\n", out.taskId.String(), out.tupleId, nextStage, nextTask, out.output)

					if err != nil { // Write didn't go through, disconnect and try again
						_ = client.Conn.Close()
						worker.connectionsLock.Lock()
						delete(worker.connections, nextWorker)
						worker.connectionsLock.Unlock()
						worker.taskOutputs <- out
						continue
					}

					// Wait for the ack
					_ = client.Conn.SetReadDeadline(time.Now().Add(clientTimeout))
					ack, err := client.Buf.ReadString('\n')
					expectedAck := fmt.Sprintf("%s-%d-%s", out.taskId.String(), out.tupleId, ACK)
					if err != nil || strings.TrimSpace(ack) != expectedAck {
						worker.taskOutputs <- out // didn't receive the ack, just try again
					}
				} else { // output data to the distributed file system
					// Send the tuple to the leader, they will write to HyDFS
					if worker.tupleSendConn == nil {
						fmt.Println("CRITICAL ERROR: tupleSendConn is nil! Initialize hasn't run yet.")
						worker.taskOutputs <- out          // Re-queue the tuple so it isn't lost
						time.Sleep(100 * time.Millisecond) // Wait for Initialize
						continue
					}
					_, err = fmt.Fprintln(worker.tupleSendConn, out.output)
					if err != nil {
						fmt.Println("error sending tuple to leader", err)
						worker.taskOutputs <- out // Re-queue the tuple so it isn't lost
					}
				}
			}
		}()

		// Goroutine for reading in tuples
		go func() {
			tupleListener, err := net.Listen("tcp", TuplePort)
			if err != nil {
				return
			}
			defer func(tupleListener net.Listener) {
				_ = tupleListener.Close()
			}(tupleListener)

			for {
				conn, err := tupleListener.Accept()
				if err != nil {
					continue
				}
				go func(conn net.Conn) {
					defer conn.Close()
					reader := bufio.NewReader(conn)
					for {
						tuple, err := reader.ReadString('\n')
						if err != nil {
							return // connection closed/failed
						}
						split := strings.SplitN(tuple, ",", 4)

						// find the correct task
						stage, err := strconv.Atoi(split[1])
						if err != nil {
							continue
						}
						task, err := strconv.Atoi(split[2])
						if err != nil {
							continue
						}
						targetTask := TaskID{Stage: stage, Task: task}

						// De-duplication
						worker.tuplesLock.Lock()
						if _, ok := worker.receivedTuples[split[0]]; ok {
							// Log rejection
							worker.tasksLocker.RLock()
							if t, ok := worker.tasks[targetTask]; ok && t.logFile != nil {
								_, _ = fmt.Fprintln(t.logFile, "REJECTED (Duplicate):", split[0])
							}
							worker.tasksLocker.RUnlock()

							// We have already received this tuple, send an ack back
							ackMsg := fmt.Sprintf("%s-%s\n", split[0], ACK)
							_, _ = fmt.Fprintf(conn, ackMsg)
							continue
						} else {
							worker.receivedTuples[split[0]] = true
						}
						worker.tuplesLock.Unlock()

						// write to task
						worker.tasksLocker.Lock()
						_, err = io.WriteString(worker.tasks[targetTask].input, split[3])
						if worker.tasks[targetTask].inputRate == 0 {
							worker.tasks[targetTask].startTime = time.Now()
						}
						worker.tasks[targetTask].inputRate++
						worker.tasksLocker.Unlock()
						if err != nil {
							continue // we weren't able to write, so no ack
						}
						// send the ack
						_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
						ackMsg := fmt.Sprintf("%s-%s\n", split[0], ACK)
						_, err = fmt.Fprintf(conn, ackMsg)
						if err != nil {
							continue
						}
						//var r []resources.AppendReply
						//worker.hydfsClient.Go("Client.RemoteAppend", &resources.RemoteFileArgs{
						//	RemoteName: fmt.Sprintf("%s_%d-%d", worker.rainStormStartTime, stage, task),
						//	Content:    []byte(fmt.Sprintf("RECEIVED,%s,%s\n", split[0], split[3])),
						//}, &r, nil)
						worker.logChan <- logRequest{
							fileName: fmt.Sprintf("%s_%d-%d", worker.rainStormStartTime, stage, task),
							data:     fmt.Sprintf("RECEIVED,%s,%s", split[0], split[3]),
						}
					}
				}(conn)
			}
		}()

		// Local Resource Manager
		go func() {
			ticker := time.Tick(time.Second)

			for {
				<-ticker
				worker.tasksLocker.RLock()
				for t, task := range worker.tasks {

					if task.startTime.IsZero() {
						continue
					}

					// If this is the first check, snapshot the current state and wait for the next tick
					if task.lastCheckTime.IsZero() {
						task.lastCheckTime = time.Now()
						task.lastInputCount = task.inputRate
						continue
					}

					// (Current Total - Old Total) / (Now - Old Time)
					now := time.Now()
					duration := now.Sub(task.lastCheckTime).Seconds()

					if duration > 0 {
						// Calculate tuples per second over the last window
						tuplesReceivedSinceLastTick := float64(task.inputRate - task.lastInputCount)
						rate := tuplesReceivedSinceLastTick / duration

						// Update snapshot for the next tick
						task.lastCheckTime = now
						task.lastInputCount = task.inputRate

						// Send rate update to leader
						var r int
						worker.rainStormLeader.Go("RainStorm.ReceiveRateUpdate", RmUpdate{
							Stage: t.Stage,
							Rate:  rate,
							Task:  t.Task,
						}, &r, nil)
					}
				}
				worker.tasksLocker.RUnlock()
			}
		}()

		// Goroutine for writing to log files
		go func() {
			// Map to buffer data for each file: map[filename]*bytes.Buffer
			buffers := make(map[string]*bytes.Buffer)
			ticker := time.Tick(500 * time.Millisecond) // Flush interval

			for {
				select {
				case <-ticker:
					for name, buf := range buffers {
						if buf.Len() > 0 {
							var r []resources.AppendReply
							// Copy data to avoid race conditions during reset
							data := make([]byte, buf.Len())
							copy(data, buf.Bytes())

							// Async RPC to append the batch
							worker.hydfsClient.Go("Client.RemoteAppend", &resources.RemoteFileArgs{
								RemoteName: name,
								Content:    data,
							}, &r, nil)

							// Clear the buffer
							buf.Reset()
						}
					}
				case req := <-worker.logChan:
					// Get or create a buffer for this specific file
					buf, ok := buffers[req.fileName]
					if !ok {
						buf = new(bytes.Buffer)
						buffers[req.fileName] = buf
					}
					// Add the log line to the buffer
					buf.WriteString(req.data)
					// Flush when the buffer gets too big
					if buf.Len() > 10240 {
						var r []resources.AppendReply
						data := make([]byte, buf.Len())
						copy(data, buf.Bytes())
						worker.hydfsClient.Go("Client.RemoteAppend", &resources.RemoteFileArgs{
							RemoteName: req.fileName,
							Content:    data,
						}, &r, nil)
						buf.Reset()
					}
				}
			}
		}()

		<-worker.done
		println("This job finished")
		_ = leaderListener.Close()
		_ = worker.rainStormLeader.Close()
		_ = worker.tupleSendConn.Close()
		time.Sleep(1 * time.Second) // wait for os to release port 8021
	}

}

// getOutboundIP gets the preferred outbound Ip of this machine, source: https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = conn.Close()
	}()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.To4()
}

func (w *Worker) ReceiveFinishedStage(stage int, reply *int) error {
	w.tasksLocker.RLock()
	var inputsToClose []io.WriteCloser
	for key, value := range w.tasks {
		if key.Stage == stage+1 {
			inputsToClose = append(inputsToClose, value.input)
		}
	}
	w.tasksLocker.RUnlock()

	for _, input := range inputsToClose {
		_ = input.Close()
	}
	if stage == len(w.ips)-1 {
		w.done <- true
	}
	return nil
}

func (w *Worker) AutoscaleDown(t TaskID, reply *int) error {
	w.tasksLocker.RLock()
	defer w.tasksLocker.RUnlock()
	_ = w.tasks[t].input.Close()
	return nil
}

func (w *Worker) Initialize(args InitArgs, reply *int) error {
	w.stageOperations = args.Ops
	w.rainStormStartTime = args.Time.Format("20060102150405")
	w.hydfsDestFile = args.HyDFSDestFile
	w.lowWatermark = args.LowWatermark
	w.highWatermark = args.HighWatermark
	rainStormLeader, _ := rpc.Dial("tcp", "fa25-cs425-1401.cs.illinois.edu"+GlobalRMPort)
	w.rainStormLeader = rainStormLeader
	tupleSendConn, err := net.Dial("tcp", "fa25-cs425-1401.cs.illinois.edu"+TuplePort)
	if err != nil {
		fmt.Println(err)
	}
	w.tupleSendConn = tupleSendConn
	return nil
}

func (w *Worker) ReceiveIPs(ips []map[int]*TaskInfo, reply *int) error {
	w.taskIDLocker.Lock()
	defer w.taskIDLocker.Unlock()

	w.ips = ips
	w.sortedTaskIDs = make([][]int, len(ips))

	// update sortedTaskIDs
	for stage, tasks := range ips {
		w.sortedTaskIDs[stage] = make([]int, 0, len(tasks))
		for task := range tasks {
			w.sortedTaskIDs[stage] = append(w.sortedTaskIDs[stage], task)
		}
		slices.Sort(w.sortedTaskIDs[stage])
	}
	return nil
}

func (w *Worker) AddTask(t Task, reply *int) error {
	// Set up the task and its pipes
	cmdArgs := strings.Fields(t.Executable.Args)
	task := exec.Command(string(t.Executable.Name), cmdArgs...)
	taskStdin, err := task.StdinPipe()
	if err != nil {
		return err
	}

	taskStdout, err := task.StdoutPipe()
	if err != nil {
		return err
	}

	err = task.Start()
	if err != nil {
		return err
	}
	homeDir, _ := os.UserHomeDir()
	logDir := filepath.Join(homeDir, "taskLogs")
	_ = os.MkdirAll(logDir, 0755) // Create dir if missing
	logFile, err := os.OpenFile(filepath.Join(logDir, fmt.Sprintf("task_%d_%d_%s", t.Stage, t.TaskNumber, time.Now().Format("20060102150405"))), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(logFile, "Starting task:", t.Executable.Name, "with pid: ", task.Process.Pid)

	// Connect the task's pipe to the channel
	tId := taskToTaskId(t)
	go func(pipe io.Reader, t TaskID, c chan<- taskOutput, cmd *exec.Cmd) {
		scanner := bufio.NewScanner(pipe)
		counter := 0
		for scanner.Scan() {
			c <- taskOutput{
				tupleId: counter,
				taskId:  t,
				output:  scanner.Text(),
			}
			counter++
		}

		if scanner.Err() != nil {
			fmt.Println("Scanner error:", scanner.Err())
			return
		}

		err = cmd.Wait()
		if err == nil {
			var reply int
			println("told leader im done", t.String())
			err = w.rainStormLeader.Call("RainStorm.ReceiveTaskCompletion", t, &reply)
			if err != nil {
				println(err.Error())
			}
			println("leader responded", t.String())
		} else {
			var reply int
			_ = w.rainStormLeader.Call("RainStorm.ReceiveFailure", t, &reply)
			fmt.Printf("Task %v failed: %v\n", t, err)
		}
		w.tasksLocker.Lock()
		if storedTask, ok := w.tasks[t]; ok && storedTask.cmd == task {
			_ = storedTask.logFile.Close()
			delete(w.tasks, t)
		}
		w.tasksLocker.Unlock()
	}(taskStdout, tId, w.taskOutputs, task)

	// Add the task to the map
	w.tasksLocker.Lock()
	w.tasks[tId] = &localTask{
		cmd:     task,
		input:   taskStdin,
		output:  taskStdout,
		logFile: logFile,
	}
	w.tasksLocker.Unlock()

	// Check if the task has any tuples it needs to recover

	// First, check if this is the first time this task is getting created
	var createReply []resources.AddFileReply
	taskLogFile := fmt.Sprintf("%s_%d-%d", w.rainStormStartTime, t.Stage, t.TaskNumber)
	err = w.hydfsClient.Call("Client.RemoteCreate", &resources.RemoteFileArgs{
		RemoteName: taskLogFile,
		Content:    make([]byte, 0),
	}, &createReply)
	if err != nil {
		return err
	}
	recoveredTask := false
	for _, fileReply := range createReply {
		var e *resources.FileAlreadyExistsError
		if errors.As(fileReply.Err, &e) { // file already exists, so this is a recovery
			recoveredTask = true
		}
	}

	if recoveredTask {
		// Need to go through the log file and get all the tuples that haven't been processed yet
		var contents []byte
		err = w.hydfsClient.Call("Client.RemoteGet", taskLogFile, &contents)
		scanner := bufio.NewScanner(bytes.NewReader(contents))

		// Mark all processed tuples
		w.tuplesLock.Lock()
		tuples := make(map[string]string)      // Key=ID, Value=tuple
		tupleStatus := make(map[string]string) // Key=ID, Value="RECEIVED" or "PROCESSED"
		for scanner.Scan() {
			splits := strings.SplitN(scanner.Text(), ",", 3)
			if len(splits) != 3 {
				continue
			}
			id := splits[1]
			w.receivedTuples[id] = true
			if splits[0] == "PROCESSED" {
				tupleStatus[id] = "PROCESSED"
			} else if splits[0] == "RECEIVED" {
				if _, exists := tupleStatus[id]; !exists {
					tupleStatus[id] = "RECEIVED"
					tuples[id] = splits[2]
				}
			}
		}
		w.tuplesLock.Unlock()

		// Add all unmarked tuples
		for _, tuple := range tuples {
			_, err = io.WriteString(taskStdin, tuple+"\n")
			if err != nil {
				fmt.Println("Error writing tuple to task ", tuple, err)
			}
		}
	}
	*reply = task.Process.Pid
	return nil
}

func (w *Worker) KillTask(t Task, reply *int) error {
	// TODO: use PID
	w.tasksLocker.Lock()
	defer w.tasksLocker.Unlock()
	id := taskToTaskId(t)
	task, ok := w.tasks[id]
	if ok {
		_ = task.cmd.Process.Kill()
		_ = task.input.Close()
		_ = task.output.Close()
	}
	return nil
}

func taskToTaskId(t Task) TaskID {
	return TaskID{
		Stage: t.Stage,
		Task:  t.TaskNumber,
	}
}
