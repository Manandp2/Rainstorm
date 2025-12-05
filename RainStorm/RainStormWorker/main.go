package main

import (
	"bufio"
	"fmt"
	. "g14-mp4/RainStorm/resources"
	"g14-mp4/mp3/resources"
	"io"
	"net"
	"net/rpc"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type localTask struct {
	cmd    *exec.Cmd
	input  io.WriteCloser // To send tuples to tasks (receives data from tcp)
	output io.ReadCloser  // To send tuples to the next stage (sends data through tcp)
}
type taskID struct {
	stage int
	task  int
}

func (t *taskID) String() string {
	return fmt.Sprintf("%d-%d", t.stage, t.task)
}

type taskOutput struct {
	tupleId int
	taskId  taskID
	output  string
}
type Worker struct {
	rainStormLeader    *rpc.Client // used to send task completions
	rainStormStartTime string
	hydfsClient        *rpc.Client

	done        chan bool
	tasksLocker sync.Mutex
	tasks       map[taskID]localTask

	ips           []map[int]net.IP // ips of machines with [stage][task] indexing
	taskIDLocker  sync.RWMutex
	sortedTaskIDs [][]int // used to find the task # within a given stage

	taskOutputs     chan taskOutput
	connections     map[string]*WorkerClient
	connectionsLock sync.RWMutex
	stageOperations []Operation

	receivedTuples map[string]bool // key = taskId-TupleId, value is dummy
}

type WorkerClient struct {
	conn net.Conn
	buf  *bufio.Reader
}

const clientTimeout = time.Second * 3
const ACK = "ACK"

func main() {
	leader, err := rpc.Dial("tcp", "fa25-cs425-1401.cs.illinois.edu"+IntroducePort)
	if err != nil {
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
		rainStormLeader, err := rpc.Dial("tcp", "fa25-cs425-1401.cs.illinois.edu"+GlobalRMPort)
		if err != nil {
			continue // try again
		}
		worker := Worker{
			rainStormLeader: rainStormLeader,
			hydfsClient:     hydfsClient,
			done:            make(chan bool),
			tasks:           make(map[taskID]localTask),
			taskOutputs:     make(chan taskOutput, 100),
			connections:     make(map[string]*WorkerClient),
		}
		err = server.Register(&worker)
		if err != nil {
			return
		}
		leaderListener, err := net.Listen("tcp", AssignmentPort)
		if err != nil {
			fmt.Println(err)
			time.Sleep(1 * time.Second)
			continue
		}
		go server.Accept(leaderListener)

		// Goroutine for sending out tuples
		go func() {
			for {
				// On output of tuple from a task, send it to the next task
				out := <-worker.taskOutputs
				nextStage := out.taskId.stage + 1
				// TODO: write processed for current tuple to HyDFS
				var r resources.AppendReply
				worker.hydfsClient.Go("Client.RemoteAppend", &resources.RemoteFileArgs{
					RemoteName: fmt.Sprintf("%s_%d-%d", worker.rainStormStartTime, out.taskId.stage, out.taskId.task),
					Content:    []byte(fmt.Sprintf("PROCESSED-%s", out.output)),
				}, &r, nil)
				if nextStage < len(worker.ips) { // send it to the next stage
					key := out.output
					if worker.stageOperations[nextStage].Name == AggregateByKey {
						hashIndex, err := strconv.Atoi(worker.stageOperations[nextStage].Args)
						if err != nil {
							hashIndex = 0
						}
						key = strings.Split(out.output, ",")[hashIndex]
					}

					// Find which client gets the next tuple
					worker.taskIDLocker.RLock()
					nextStageTasks := worker.sortedTaskIDs[nextStage]
					hash := HashString(key)
					if hash < 0 { // make sure hash is positive
						hash = -hash
					}
					nextTask := nextStageTasks[hash%len(nextStageTasks)] // Go to the sorted array and find the task #

					nextWorker := worker.ips[nextStage][nextTask].String()
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
							conn: conn,
							buf:  bufio.NewReader(conn),
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
					_ = client.conn.SetWriteDeadline(time.Now().Add(clientTimeout))
					// Id-Id,stage, task, data
					_, err = fmt.Fprintf(client.conn, "%s-%d,%d,%d,%s\n", out.taskId.String(), out.tupleId, nextStage, nextTask, out.output)

					if err != nil { // Write didn't go through, disconnect and try again
						_ = client.conn.Close()
						worker.connectionsLock.Lock()
						delete(worker.connections, nextWorker)
						worker.connectionsLock.Unlock()
						worker.taskOutputs <- out
						continue
					}

					// Wait for the ack
					_ = client.conn.SetReadDeadline(time.Now().Add(clientTimeout))
					ack, err := client.buf.ReadString('\n')
					expectedAck := fmt.Sprintf("%s-%d-%s", out.taskId.String(), out.tupleId, ACK)
					if err != nil || strings.TrimSpace(ack) != expectedAck {
						worker.taskOutputs <- out // didn't receive the ack, just try again
					}
				} else { // output data to the distributed file system
					// TODO: Write to HyDFS
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

						// De-duplication
						if _, ok := worker.receivedTuples[split[0]]; ok {
							// We have already received this tuple, send an ack back
							ackMsg := fmt.Sprintf("%s-%s\n", split[0], ACK)
							_, _ = fmt.Fprintf(conn, ackMsg)
							continue
						} else {
							worker.receivedTuples[split[0]] = true
						}

						// find the correct task
						stage, err := strconv.Atoi(split[1])
						if err != nil {
							continue
						}
						task, err := strconv.Atoi(split[2])
						if err != nil {
							continue
						}

						// write to task
						targetTask := taskID{stage: stage, task: task}
						worker.tasksLocker.Lock()
						_, err = io.WriteString(worker.tasks[targetTask].input, split[3])
						worker.tasksLocker.Unlock()
						if err != nil {
							continue // we weren't able to write, so no ack
						}
						// send the ack
						ackMsg := fmt.Sprintf("%s-%s\n", split[0], ACK)
						_, err = fmt.Fprintf(conn, ackMsg)
						if err != nil {
							continue
						}
						var r resources.AppendReply
						// TODO: write received for current tuple to HyDFS
						worker.hydfsClient.Go("Client.RemoteAppend", &resources.RemoteFileArgs{
							RemoteName: fmt.Sprintf("%s_%d-%d", worker.rainStormStartTime, stage, task),
							Content:    []byte(fmt.Sprintf("RECIEVED-%s", split[3])),
						}, &r, nil)
					}
				}(conn)
			}
		}()

		<-worker.done
		_ = leaderListener.Close()
		_ = rainStormLeader.Close()
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
	w.tasksLocker.Lock()
	defer w.tasksLocker.Unlock()
	for key, value := range w.tasks {
		if key.stage == stage+1 {
			_ = value.input.Close()
		}
	}
	return nil
}

func (w *Worker) AutoscaleDown(t taskID, reply *int) error {
	w.tasksLocker.Lock()
	defer w.tasksLocker.Unlock()
	_ = w.tasks[t].input.Close()
	return nil
}

func (w *Worker) Initialize(stageOps []Operation, reply *int) error {
	w.stageOperations = stageOps
	// TODO: get starting time of rainstorm task, use time.Format("20060102150405")
	return nil
}

func (w *Worker) ReceiveIPs(ips []map[int]net.IP, reply *int) error {
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
	task := exec.Command(string(t.Executable.Name), t.Executable.Args)
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
	// TODO: go through log file for task and add any received but not processed tuples to taskStdin
	// First check if this is the first time this task is getting created
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
		if fileReply.Err != nil { // file already exists
			recoveredTask = true
		}
	}
	if recoveredTask { // Need to go through the log file and get all the tuples that haven't been processed yet
		var contents []byte
		err = w.hydfsClient.Call("Client.RemoteGet", taskLogFile, &contents)
	}
	tId := taskToTaskId(t)
	go func(pipe io.Reader, t taskID, c chan<- taskOutput, cmd *exec.Cmd) {
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
			_ = w.rainStormLeader.Call("RainStorm.ReceiveTaskCompletion", t, &reply)
		} else {
			fmt.Printf("Task %v failed: %v\n", t, err)
		}
	}(taskStdout, tId, w.taskOutputs, task)

	w.tasksLocker.Lock()
	w.tasks[tId] = localTask{
		cmd:    task,
		input:  taskStdin,
		output: taskStdout,
	}
	w.tasksLocker.Unlock()
	return nil
}

func (w *Worker) KillTask(t Task, reply *int) error {
	w.tasksLocker.Lock()
	defer w.tasksLocker.Unlock()
	id := taskToTaskId(t)
	task, ok := w.tasks[id]
	if ok {
		_ = task.cmd.Process.Kill()
		_ = task.input.Close()
		_ = task.output.Close()
		delete(w.tasks, id)
	}
	return nil
}

func taskToTaskId(t Task) taskID {
	return taskID{
		stage: t.Stage,
		task:  t.TaskNumber,
	}
}
