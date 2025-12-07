package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	. "g14-mp4/RainStorm/resources"
	"g14-mp4/mp3/resources"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WorkerIps struct {
	ips []net.IP
	l   sync.RWMutex
}

type RainStorm struct {
	NumStages                int
	NumTasksPerStage         int
	HydfsSrcDirectory        string
	HydfsDestinationFileName string
	ExactlyOnce              bool
	AutoScale                bool
	InputRate                float64
	LowestRate               float64
	HighestRate              float64
	Ops                      []Operation
	TaskInformation          []map[int]*TaskInfo // [stage][task] --> IP, PID
	NextTaskNum              []int               // [stage]
	NextAvailableVM          int
	Stage1UpdatesChan        chan map[int]net.IP
	Lock                     *sync.RWMutex
	DoneReading              bool
	StartTime                time.Time
	LogFile                  *os.File
	LogFileChan              chan string
}

const clientTimeout = time.Second * 3

var workers WorkerIps
var numWorkers int
var numSuccessfulDials int
var rpcWorkers map[string]*rpc.Client
var rpcWorkersLock sync.RWMutex
var appCompletedChan chan bool
var dataDir string

func main() {
	homeDir, _ := os.UserHomeDir()
	dataDir = filepath.Join(homeDir, "data")
	workers = WorkerIps{}
	appCompletedChan = make(chan bool, 1)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := rpc.Register(&workers)
		if err != nil {
			fmt.Println(err)
			return
		}
		listener, err := net.Listen("tcp", IntroducePort)
		if err != nil {
			fmt.Println(err)
			return
		}
		rpc.Accept(listener)
	}()
	input := make(chan RainStorm)
	go processStdin(input)
	hydfsClient, err := rpc.Dial("tcp", "localhost:8011") // connect to our own HydFS client
	if err != nil {
		fmt.Println("Failed to connect to HyDFS client from leader: " + err.Error())
	}
	defer hydfsClient.Close()

	for {
		r := <-input
		if numWorkers == 0 {
			panic("No workers")
		}

		r.LogFileChan = make(chan string, 100)
		r.StartTime = time.Now()

		tupleListener, err := net.Listen("tcp", TuplePort)
		if err != nil {
			return
		}
		//logger
		go func() {
			path := filepath.Join(homeDir, "RainStormLogs", "RainStorm_"+r.StartTime.Format("20060102150405"))
			_ = os.MkdirAll(filepath.Join(homeDir, "RainStormLogs"), 0755)
			r.LogFile, _ = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
			_, _ = r.LogFile.WriteString(r.StartTime.Format("2006-01-02 15:04:05") + ": Started RainStorm Application\n")
			writer := bufio.NewWriter(r.LogFile)
			defer func() {
				writer.Flush()
				r.LogFile.Close()
			}()
			for {
				select {
				case <-ctx.Done():
					writer.WriteString(time.Now().Format("2006-01-02 15:04:05") + ": RainStorm Application Completed\n")
					return
				case s, ok := <-r.LogFileChan:
					if !ok {
						//channel closed
						writer.WriteString(time.Now().Format("2006-01-02 15:04:05") + ": RainStorm Application Completed\n")
						return
					}
					writer.WriteString(time.Now().Format("2006-01-02 15:04:05") + ": " + s)
					writer.Flush()
				}
			}
		}()
		// INITIATE NEW RAINSTORM APPLICATION
		//Global RM
		/*
			1. open listener for current task input rates from workers
			2. check if autoscale is on, if it is ->
			3. compare rates to see if changes are needed
			4. complete changes
		*/
		appServer := rpc.NewServer()
		err = appServer.Register(&r)
		if err != nil {
			fmt.Println(err)
			continue
		}
		globalRmListener, err := net.Listen("tcp", GlobalRMPort)
		if err != nil {
			fmt.Println("GlobalRM unable to connect to worker: " + err.Error())
			continue
		}
		go func() {
			for {
				conn, err := globalRmListener.Accept()
				if err != nil {
					if strings.Contains(err.Error(), "use of closed network connection") {
						return // Exit quietly
					}
					fmt.Println("GlobalRM Accept error: " + err.Error())
					return
				}
				//give connection to RPC server
				go appServer.ServeConn(conn)
			}
		}()

		workers.l.RLock()
		rpcWorkers = make(map[string]*rpc.Client)
		numSuccessfulDials = 0
		rpcWorkersLock.Lock()
		for _, workerIp := range workers.ips {
			//collect list of tasks for this worker
			worker, err := rpc.Dial("tcp", workerIp.String()+AssignmentPort)
			if err != nil {
				fmt.Println("Unable to connect to worker: " + err.Error())
				continue
			}
			rpcWorkers[workerIp.String()] = worker
			numSuccessfulDials++
		}
		workers.l.RUnlock()
		rpcWorkersLock.Unlock()

		r.Lock = new(sync.RWMutex)
		r.Lock.Lock()
		r.TaskInformation = make([]map[int]*TaskInfo, r.NumStages)
		r.NextTaskNum = make([]int, r.NumStages)
		r.Stage1UpdatesChan = make(chan map[int]net.IP, 20)
		r.DoneReading = false
		//r.TaskCompletion = make([]CompletionTuple, r.NumStages)
		r.initWorker()
		r.NextAvailableVM = 0
		for i := range r.NumStages {
			r.TaskInformation[i] = make(map[int]*TaskInfo)
			//r.TaskCompletion[i] = CompletionTuple{
			//	Counter:      0,
			//	StateTracker: make(map[int]bool),
			//}
			for j := range r.NumTasksPerStage {
				r.addTask(i, j)
				r.NextTaskNum[i]++
			}
		}
		r.sendIps()
		r.Lock.Unlock()

		//@TODO: read srcFile from HyDFS and send into system at Input Rate for this application
		// send stage -1 is done once done reading from the file
		// read in from local; output on HyDFS
		var createReply []resources.AddFileReply
		err = hydfsClient.Call("Client.RemoteCreate", &resources.RemoteFileArgs{
			RemoteName: r.HydfsDestinationFileName,
			Content:    make([]byte, 0),
		}, &createReply)
		if err != nil {
			fmt.Println(err.Error())
		}

		inputFile, err := os.Open(filepath.Join(dataDir, r.HydfsSrcDirectory))
		if err != nil {
			fmt.Println("Unable to open src directory: " + err.Error())
		}
		//buffered write to HyDFS output file
		outputChan := make(chan string, 200)
		go func() {
			buffer := bytes.Buffer{}
			for {
				select {
				case <-ctx.Done():
					if buffer.Len() > 0 {
						var reply []resources.AppendReply
						_ = hydfsClient.Call("Client.RemoteAppend", &resources.RemoteFileArgs{
							RemoteName: r.HydfsDestinationFileName,
							Content:    buffer.Bytes(),
						}, &reply)
						buffer.Reset()
					}
					return
				case line := <-outputChan:
					buffer.WriteString(line)
					if buffer.Len() > 4096 {
						var reply []resources.AppendReply
						_ = hydfsClient.Call("Client.RemoteAppend", &resources.RemoteFileArgs{
							RemoteName: r.HydfsDestinationFileName,
							Content:    buffer.Bytes(),
						}, &reply)
						buffer.Reset()
					}
				}
			}
		}()

		//listen for tuples to print to console and buffered append to hydfs
		go func() {
			for {
				select {
				case <-ctx.Done():
					_ = tupleListener.Close()
					return
				default:
					conn, err := tupleListener.Accept()
					if err != nil {
						continue
					}
					go func(conn net.Conn) {
						defer conn.Close()
						reader := bufio.NewReader(conn)
						for {
							select {
							case <-ctx.Done():
								return
							default:
								line, err := reader.ReadString('\n')
								if err != nil {
									return // connection closed/failed
								}
								fmt.Print(line)
								outputChan <- line
							}
						}
					}(conn)
				}
			}
		}()

		// reading src file and sending lines to tasks
		go func() {
			scanner := bufio.NewScanner(inputFile)
			r.Lock.Lock()
			firstStageTasks := make(map[int]net.IP)
			firstTaskList := make([]int, 0)
			for tid, info := range r.TaskInformation[0] {
				firstStageTasks[tid] = info.Ip
				firstTaskList = append(firstTaskList, tid)
			}
			sort.Ints(firstTaskList)
			r.Lock.Unlock()
			tupleClients := make(map[string]*WorkerClient, numWorkers)
			startTime := time.Now()
			var numProcessed float64 = 0
			readingChan := make(chan struct {
				line    string
				lineNum int
			}, 100)
			go func() {
				lineNum := 0
				for scanner.Scan() {
					readingChan <- struct {
						line    string
						lineNum int
					}{line: scanner.Text(), lineNum: lineNum}
					lineNum++
				}
				readingChan <- struct {
					line    string
					lineNum int
				}{line: "", lineNum: -1}
				r.Lock.Lock()
				r.DoneReading = true
				r.Lock.Unlock()
			}()
			eofReceived := false
			done := false
			for {
				select {
				case tuple := <-readingChan:
					if tuple.lineNum == -1 {
						//done reading
						eofReceived = true
						continue
					}
					select {
					case updatedMap := <-r.Stage1UpdatesChan:
						firstStageTasks = updatedMap
						firstTaskList = make([]int, 0)
						for k := range firstStageTasks {
							firstTaskList = append(firstTaskList, k)
						}
						sort.Ints(firstTaskList)
					default:
					}

					nextTask := firstTaskList[tuple.lineNum%len(firstTaskList)]
					nextTaskIp := firstStageTasks[nextTask]

					client, ok := tupleClients[nextTaskIp.String()]
					if !ok {
						conn, err := net.Dial("tcp", nextTaskIp.String()+TuplePort)
						if err != nil {
							fmt.Println("Unable to connect to worker: " + err.Error())
							delete(tupleClients, nextTaskIp.String())
							continue
						}
						client = &WorkerClient{
							Conn: conn,
							Buf:  bufio.NewReader(conn),
						}
						tupleClients[nextTaskIp.String()] = client
					}

					// Send the tuple
					_ = client.Conn.SetWriteDeadline(time.Now().Add(clientTimeout))
					// Id-Id, stage, task, data
					_, _ = fmt.Fprintf(client.Conn, "%s-%d,%d,%d,%s\n", "temp", tuple.lineNum, 0, nextTask, tuple.line)

					// Wait for the ack
					_ = client.Conn.SetReadDeadline(time.Now().Add(clientTimeout))
					ack, err := client.Buf.ReadString('\n')
					expectedAck := fmt.Sprintf("%s-%d-%s", "temp", tuple.lineNum, "ACK")
					if err != nil || strings.TrimSpace(ack) != expectedAck {
						client.Conn.Close()
						delete(tupleClients, nextTaskIp.String())
						readingChan <- tuple
						continue
					}

					expectedDuration := time.Duration((numProcessed / r.InputRate) * float64(time.Second))
					targetTime := startTime.Add(expectedDuration)

					now := time.Now()
					if targetTime.After(now) {
						// ahead of schedule, sleep to sync with desired rate
						time.Sleep(targetTime.Sub(now))
					}
					numProcessed++
				default:
					// reached if channel is empty
					if eofReceived {
						done = true
					}
				}
				if done {
					break
				}
			}

			r.sendStageCompletion(-1)
			for _, c := range tupleClients {
				c.Conn.Close()
			}
			_ = inputFile.Close()
		}()
		// needs to wait for the application to complete before cleaning up --> @TODO: come back to this
		<-appCompletedChan //blocking
		println("RainStorm Application completed!")
		// CLEANUP: do once the current RainStorm application is done
		cancel()
		close(r.LogFileChan)
		rpcWorkersLock.Lock()
		for _, worker := range rpcWorkers {
			_ = worker.Close()
		}
		rpcWorkersLock.Unlock()

		err = globalRmListener.Close()
		if err != nil {
			fmt.Println(err)
		}
	}

}

func (w *WorkerIps) AddWorker(args net.IP, reply *int) error {
	workers.l.Lock()
	defer workers.l.Unlock()
	workers.ips = append(workers.ips, args)
	numWorkers++
	return nil
}

func (app *RainStorm) ReceiveFailure(task Task, reply *int) error {
	// restart the task on the next worker in the cycle
	app.Lock.Lock()
	defer app.Lock.Unlock()
	if _, exists := app.TaskInformation[task.Stage][task.TaskNumber]; !exists {
		fmt.Printf("Failing task:%d at stage: %d does not exist", task.TaskNumber, task.Stage)
	} else {
		workers.l.RLock()
		app.TaskInformation[task.Stage][task.TaskNumber].Ip = workers.ips[app.NextAvailableVM%numWorkers]
		workers.l.RUnlock()
		app.NextAvailableVM++
		if task.Stage == 0 && !app.DoneReading {
			temp := make(map[int]net.IP)
			for t, ip := range app.TaskInformation[0] {
				temp[t] = ip.Ip
			}
			app.Stage1UpdatesChan <- temp
		}
		app.LogFileChan <- fmt.Sprintf("Restarting Task at VM: %s PID: %d op_exe: %s\n", app.TaskInformation[task.Stage][task.TaskNumber].Ip.String(), reply, string(app.Ops[task.Stage].Name))
		app.addTask(task.Stage, task.TaskNumber)
		app.sendIps()
	}
	return nil
}
func (app *RainStorm) ReceiveRateUpdate(args RmUpdate, reply *int) error {
	//@TODO: write to leader logs when receiving a tuple rate
	//app.LogFile
	app.LogFileChan <- fmt.Sprintf("Rate: %.2f TaskID: %d Stage %d\n", args.Rate, args.Task, args.Stage)
	if app.AutoScale {
		if args.Rate < app.LowestRate {
			//	add a task to this stage
			app.Lock.Lock()
			taskNum := app.NextTaskNum[args.Stage]
			app.NextTaskNum[args.Stage]++
			app.LogFileChan <- fmt.Sprintf("Upscaling Stage: %d Rate: %.2f\n", args.Stage, args.Rate)
			app.addTask(args.Stage, taskNum)
			app.sendIps()
			app.Lock.Unlock()
		} else if args.Rate > app.HighestRate {
			//	remove a task from this stage
			app.Lock.Lock()
			app.LogFileChan <- fmt.Sprintf("Downscaling Stage: %d Rate: %.2f\n", args.Stage, args.Rate)
			app.removeTask(args.Stage)
			app.Lock.Unlock()
		}
	}
	return nil
}

func (app *RainStorm) ReceiveTaskCompletion(args TaskID, reply *int) error {
	//stage completion manager --> manage markers from tasks saying they are done
	app.Lock.Lock()
	defer app.Lock.Unlock()
	if _, exists := app.TaskInformation[args.Stage][args.Task]; exists {
		app.LogFileChan <- fmt.Sprintf("Task Completed TaskID: %d Stage: %d VM: %s PID: %d op_exe: %s\n", args.Task, args.Stage, app.TaskInformation[args.Stage][args.Task].Ip.String(), reply, string(app.Ops[args.Stage].Name))
		delete(app.TaskInformation[args.Stage], args.Task)
		//app.CurNumTasks[args.Stage] -= 1
		app.sendIps()
		if len(app.TaskInformation[args.Stage]) == 0 {
			// stage completed
			app.sendStageCompletion(args.Stage)
			if args.Stage+1 == app.NumStages {
				appCompletedChan <- true
			}
		}

	} else {
		//do nothing because this should never happen
		fmt.Printf("Received task completion for: %d, BUT should not have received this\n", args.Task)
	}
	return nil
}

func (app *RainStorm) sendStageCompletion(completedStage int) {
	waitingChan := make(chan *rpc.Call, len(rpcWorkers))
	numSuccess := 0
	rpcWorkersLock.RLock()
	for _, worker := range rpcWorkers {
		var reply int
		worker.Go("Worker.ReceiveFinishedStage", completedStage, &reply, waitingChan)
		numSuccess++
	}
	rpcWorkersLock.RUnlock()
	for i := 0; i < numSuccess; i++ {
		x := <-waitingChan
		if x.Error != nil {
			fmt.Println("Failed to send completed stageID to workers: " + x.Error.Error())
		}
	}
}

func (app *RainStorm) sendIps() { // MUST BE CALLED INSIDE RAINSTORM LOCK --> only called when current app is modified
	waitingChan := make(chan *rpc.Call, len(rpcWorkers))
	numSuccess := 0
	rpcWorkersLock.RLock()
	for _, worker := range rpcWorkers {
		var reply int
		worker.Go("Worker.ReceiveIPs", app.TaskInformation, &reply, waitingChan)
		numSuccess++
	}
	rpcWorkersLock.RUnlock()
	for i := 0; i < numSuccess; i++ {
		x := <-waitingChan
		if x.Error != nil {
			fmt.Println("Failed to send IPs to workers: " + x.Error.Error())
		}
	}
}

func (app *RainStorm) initWorker() { // MUST BE CALLED INSIDE RAINSTORM LOCK --> only called when current app is modified
	waitingChan := make(chan *rpc.Call, len(rpcWorkers))
	numSuccess := 0
	rpcWorkersLock.RLock()
	args := InitArgs{
		Ops:           app.Ops,
		Time:          app.StartTime,
		HyDFSDestFile: app.HydfsDestinationFileName,
		LowWatermark:  app.LowestRate,
		HighWatermark: app.HighestRate,
	}
	for _, worker := range rpcWorkers {
		var reply int
		worker.Go("Worker.Initialize", args, &reply, waitingChan)
		numSuccess++
	}
	rpcWorkersLock.RUnlock()
	for i := 0; i < numSuccess; i++ {
		x := <-waitingChan
		if x.Error != nil {
			fmt.Println("Failed to send list of operations to workers: " + x.Error.Error())
		}
	}
}

func (app *RainStorm) addTask(stageNum int, taskNum int) { //MUST BE WRAPPED IN LOCK WHEN CALLED
	//if taskNum > app.StageCounter[stageNum]) {
	//	app.TaskInformation[stageNum] = append(app.TaskInformation[stageNum], workers.ips[app.NextAvailableVM%numWorkers])
	//} else {
	//	app.TaskInformation[stageNum][taskNum] = workers.ips[app.NextAvailableVM%numWorkers]
	//}
	workers.l.RLock()
	app.TaskInformation[stageNum][taskNum] = &TaskInfo{Ip: workers.ips[app.NextAvailableVM%numWorkers]}
	workers.l.RUnlock()
	//app.TaskCompletion[stageNum].StateTracker[taskNum] = false
	//app.NextTaskNum[stageNum]++
	app.NextAvailableVM++
	if stageNum == 0 && !app.DoneReading {
		temp := make(map[int]net.IP)
		for task, ip := range app.TaskInformation[0] {
			temp[task] = ip.Ip
		}
		app.Stage1UpdatesChan <- temp
	}
	task := Task{
		TaskNumber: taskNum,
		Stage:      stageNum,
		Executable: app.Ops[stageNum],
	}

	var reply int
	rpcWorkersLock.RLock()
	rpcWorker := rpcWorkers[app.TaskInformation[stageNum][taskNum].Ip.String()]
	rpcWorkersLock.RUnlock()
	err := rpcWorker.Call("Worker.AddTask", task, &reply)
	if err != nil {
		fmt.Println("Failed to send request to add task: " + err.Error())
	}
	app.TaskInformation[stageNum][taskNum].Pid = reply
	//@TODO: also log the local logfile on the task
	app.LogFileChan <- fmt.Sprintf("Starting Task at VM: %s PID: %d op_exe: %s\n", app.TaskInformation[stageNum][taskNum].Ip.String(), reply, string(app.Ops[stageNum].Name))
}

func (app *RainStorm) removeTask(stageNum int) { //MUST BE WRAPPED IN APP LOCK WHEN CALLED
	if len(app.TaskInformation[stageNum]) <= 1 { // only 1 task remaining in the stage
		return
	}
	var taskNum int
	for k := range app.TaskInformation[stageNum] {
		// getting first taskNum when iterating to remove; randomized because of GO
		taskNum = k
		break
	}

	deletedTaskIp, exists := app.TaskInformation[stageNum][taskNum]
	if !exists {
		fmt.Printf("Failed to remove task: %d, stage %d: not exists", taskNum, stageNum)
		return
	}

	delete(app.TaskInformation[stageNum], taskNum)
	if stageNum == 0 && !app.DoneReading {
		temp := make(map[int]net.IP)
		for task, ip := range app.TaskInformation[0] {
			temp[task] = ip.Ip
		}
		app.Stage1UpdatesChan <- temp
	}
	app.sendIps()

	task := Task{
		TaskNumber: taskNum,
		Stage:      stageNum,
		Executable: app.Ops[stageNum],
	}
	var reply int
	rpcWorkersLock.RLock()
	rpcWorker := rpcWorkers[deletedTaskIp.Ip.String()]
	rpcWorkersLock.RUnlock()
	err := rpcWorker.Call("Worker.AutoscaleDown", task, &reply)
	if err != nil {
		fmt.Println("Failed to send request to kill task: " + err.Error())
	}
}

func processStdin(i1 chan<- RainStorm) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		stdin := scanner.Text()
		stdin = strings.TrimSpace(stdin)
		splits := strings.Split(stdin, " ")
		switch splits[0] {
		case "RainStorm": //
			var rainStorm RainStorm
			bad := false
			for i := 1; i < len(splits) && !bad; i++ {
				var err error
				switch {
				case i == 1: // NumStages
					rainStorm.NumStages, err = strconv.Atoi(splits[i])
					if err != nil {
						fmt.Println("Failed to parse NumStages: " + err.Error())
						bad = true
					}
					break
				case i == 2: // NumTasksPerStage
					rainStorm.NumTasksPerStage, err = strconv.Atoi(splits[i])
					if err != nil {
						fmt.Println("Failed to parse NumTasksPerStage: " + err.Error())
						bad = true
					}
					break
				case 3 <= i && i < len(splits)-7: // processing operations and arguments
					rainStorm.Ops = append(rainStorm.Ops, Operation{Name: OperationName(splits[i]), Args: splits[i+1]})
					i++
					break
				case i == len(splits)-7: // HydfsSrcDirectory
					rainStorm.HydfsSrcDirectory = splits[i]
					break
				case i == len(splits)-6: // HydfsDestinationFileName
					rainStorm.HydfsDestinationFileName = splits[i]
					break
				case i == len(splits)-5: // ExactlyOnce
					rainStorm.ExactlyOnce, err = strconv.ParseBool(splits[i])
					if err != nil {
						fmt.Println("Failed to parse ExactlyOnce: " + err.Error())
						bad = true
					}
					break
				case i == len(splits)-4: // AutoScale
					rainStorm.AutoScale, err = strconv.ParseBool(splits[i])
					if err != nil {
						fmt.Println("Failed to parse AutoScale: " + err.Error())
						bad = true
					}
					break
				case i == len(splits)-3: // InputRate
					rainStorm.InputRate, err = strconv.ParseFloat(splits[i], 64)
					if err != nil {
						fmt.Println("Failed to parse InputRate: " + err.Error())
						bad = true
					}
					break
				case i == len(splits)-2: // LowestRate
					rainStorm.LowestRate, err = strconv.ParseFloat(splits[i], 64)
					if err != nil {
						fmt.Println("Failed to parse LowestRate: " + err.Error())
						bad = true
					}
					break
				case i == len(splits)-1: // HighestRate
					rainStorm.HighestRate, err = strconv.ParseFloat(splits[i], 64)
					if err != nil {
						fmt.Println("Failed to parse HighestRate: " + err.Error())
						bad = true
					}
					break
				}
			}
			if !bad {
				i1 <- rainStorm
			}
			break

		case "kill_task":
			//@TODO: add implementation for this
			break

		case "list_tasks":
			break

		}
	}
}
