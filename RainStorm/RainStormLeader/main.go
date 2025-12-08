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
var curApp *RainStorm

func main() {
	homeDir, _ := os.UserHomeDir()
	dataDir = filepath.Join(homeDir, "data")
	workers = WorkerIps{}
	appCompletedChan = make(chan bool, 1)

	// RPC Register (Run once)
	go func() {
		_ = rpc.Register(&workers)
		listener, err := net.Listen("tcp", IntroducePort)
		if err != nil {
			fmt.Println("IntroducePort Error:", err)
			return
		}
		rpc.Accept(listener)
	}()

	input := make(chan RainStorm)
	go processStdin(input)
	hydfsClient, err := rpc.Dial("tcp", "localhost:8011")
	if err != nil {
		fmt.Println("Failed to connect to HyDFS:", err)
	}
	defer hydfsClient.Close()

	for {
		r := <-input
		if numWorkers == 0 {
			panic("No workers")
		}
		curApp = &r
		// Context for cancellation (Stops Listeners/Input)
		ctx, cancel := context.WithCancel(context.Background())

		// WaitGroup for PRODUCERS only (Listeners, Input)
		var wgProducers sync.WaitGroup

		r.LogFileChan = make(chan string, 100)
		r.StartTime = time.Now()

		tupleListener, err := net.Listen("tcp", TuplePort)
		if err != nil {
			continue
		}

		// --- LOGGER ---
		// (Logger has its own simple shutdown, this is fine)
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
					// Drain channel then exit
					for len(r.LogFileChan) > 0 {
						s := <-r.LogFileChan
						writer.WriteString(time.Now().Format("2006-01-02 15:04:05") + ": " + s)
					}
					writer.WriteString(time.Now().Format("2006-01-02 15:04:05") + ": RainStorm Application Completed\n")
					return
				case s, ok := <-r.LogFileChan:
					if !ok {
						return
					}
					writer.WriteString(time.Now().Format("2006-01-02 15:04:05") + ": " + s)
					writer.Flush()
				}
			}
		}()

		// --- GLOBAL RM ---
		appServer := rpc.NewServer()
		_ = appServer.Register(&r)
		globalRmListener, err := net.Listen("tcp", GlobalRMPort)
		if err != nil {
			continue
		}

		go func() {
			// Force close listener on context cancel
			go func() { <-ctx.Done(); globalRmListener.Close() }()
			for {
				conn, err := globalRmListener.Accept()
				if err != nil {
					return
				}
				go appServer.ServeConn(conn)
			}
		}()

		// ... [Dialing Workers logic remains unchanged] ...
		workers.l.RLock()
		rpcWorkers = make(map[string]*rpc.Client)
		rpcWorkersLock.Lock()
		for _, workerIp := range workers.ips {
			worker, err := rpc.Dial("tcp", workerIp.String()+AssignmentPort)
			if err != nil {
				continue
			}
			rpcWorkers[workerIp.String()] = worker
		}
		workers.l.RUnlock()
		rpcWorkersLock.Unlock()

		r.Lock = new(sync.RWMutex)
		r.Lock.Lock()
		r.TaskInformation = make([]map[int]*TaskInfo, r.NumStages)
		r.NextTaskNum = make([]int, r.NumStages)
		r.Stage1UpdatesChan = make(chan map[int]net.IP, 20)
		r.DoneReading = false
		r.initWorker()
		r.NextAvailableVM = 0
		for i := range r.NumStages {
			r.TaskInformation[i] = make(map[int]*TaskInfo)
			for j := range r.NumTasksPerStage {
				r.addTask(i, j)
				r.NextTaskNum[i]++
			}
		}
		r.sendIps()
		r.Lock.Unlock()

		// ... [HyDFS File Create logic remains unchanged] ...
		var createReply []resources.AddFileReply
		_ = hydfsClient.Call("Client.RemoteCreate", &resources.RemoteFileArgs{
			RemoteName: r.HydfsDestinationFileName,
			Content:    make([]byte, 0),
		}, &createReply)
		os.MkdirAll(filepath.Join(homeDir, "RainStormOutputs"), 0755)
		localOutputFile, _ := os.OpenFile(filepath.Join(homeDir, "RainStormOutputs", r.HydfsDestinationFileName), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)

		inputFile, err := os.Open(filepath.Join(dataDir, r.HydfsSrcDirectory))
		if err != nil {
			fmt.Println("Unable to open src directory: " + err.Error())
		}

		// --- HYDFS BUFFERED WRITER (CONSUMER) ---
		// Use a separate channel to signal when the WRITER is totally done
		writerDone := make(chan bool)
		outputChan := make(chan string, 500)

		go func() {
			buffer := bytes.Buffer{}
			flush := func() {
				if buffer.Len() > 0 {
					var reply []resources.AppendReply
					_ = hydfsClient.Call("Client.RemoteAppend", &resources.RemoteFileArgs{
						RemoteName: r.HydfsDestinationFileName,
						Content:    buffer.Bytes(),
					}, &reply)
					buffer.Reset()
				}
			}

			// Range loop runs until channel is CLOSED and EMPTY.
			// It does NOT stop on ctx.Done(). This prevents data loss.
			for line := range outputChan {
				buffer.WriteString(line)
				if buffer.Len() > 4096 {
					flush()
				}
			}
			// Flush remainder
			flush()
			// Signal main thread that we are finished
			close(writerDone)
		}()

		// --- TUPLE LISTENER (PRODUCER 1) ---
		wgProducers.Add(1)
		go func() {
			defer wgProducers.Done()

			go func() {
				<-ctx.Done()
				tupleListener.Close()
			}()

			for {
				conn, err := tupleListener.Accept()
				if err != nil {
					return
				}

				wgProducers.Add(1) // Track individual connections
				go func(c net.Conn) {
					defer wgProducers.Done()
					defer c.Close()

					// Sidecar closer
					go func() { <-ctx.Done(); c.Close() }()

					reader := bufio.NewReader(c)
					for {
						line, err := reader.ReadString('\n')

						// FIX 1: Always prioritize sending data if we got it
						if len(line) > 0 {
							//fmt.Print(line)
							localOutputFile.WriteString(line)
							outputChan <- line // Send. We know writer is alive until we close chan.
						}

						if err != nil {
							return
						}
					}
				}(conn)
			}
		}()

		// --- INPUT READER (PRODUCER 2) ---
		wgProducers.Add(1)
		go func() {
			defer wgProducers.Done()
			defer inputFile.Close()

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
			var numProcessed float64 = 0
			startTime := time.Now()

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

			// Tracks a tuple we failed to send and need to retry
			var pendingTuple *struct {
				line    string
				lineNum int
			}

			for {
				// Definition of the tuple variable for this iteration
				var tuple struct {
					line    string
					lineNum int
				}

				// --- SELECTION LOGIC ---
				if pendingTuple != nil {
					// 1. Prioritize the retry
					tuple = *pendingTuple
					pendingTuple = nil
				} else {
					// 2. Otherwise read from channel
					select {
					case <-ctx.Done():
						for _, c := range tupleClients {
							c.Conn.Close()
						}
						return
					case t := <-readingChan:
						tuple = t // FIX 1: use '=' not ':='
					}
				}

				if tuple.lineNum == -1 {
					r.sendStageCompletion(-1)
					for _, c := range tupleClients {
						c.Conn.Close()
					}
					return
				}

				// ... [Update Logic] ...
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

				// FIX 2: Handle empty task list without deadlocking channel
				if len(firstTaskList) == 0 {
					time.Sleep(50 * time.Millisecond)
					pendingTuple = &tuple // Save state
					continue              // Retry loop
				}

				nextTask := firstTaskList[tuple.lineNum%len(firstTaskList)]
				nextTaskIp := firstStageTasks[nextTask]

				client, ok := tupleClients[nextTaskIp.String()]
				if !ok {
					conn, err := net.Dial("tcp", nextTaskIp.String()+TuplePort)
					if err != nil {
						delete(tupleClients, nextTaskIp.String())
						// FIX 3: Save tuple before continuing on dial fail
						pendingTuple = &tuple
						continue
					}
					client = &WorkerClient{Conn: conn, Buf: bufio.NewReader(conn)}
					tupleClients[nextTaskIp.String()] = client
				}

				_ = client.Conn.SetWriteDeadline(time.Now().Add(clientTimeout))
				_, _ = fmt.Fprintf(client.Conn, "%s-%d,%d,%d,%s\n", "temp", tuple.lineNum, 0, nextTask, tuple.line)

				_ = client.Conn.SetReadDeadline(time.Now().Add(clientTimeout))
				ack, err := client.Buf.ReadString('\n')
				expectedAck := fmt.Sprintf("%s-%d-%s", "temp", tuple.lineNum, "ACK")

				if err != nil || strings.TrimSpace(ack) != expectedAck {
					client.Conn.Close()
					delete(tupleClients, nextTaskIp.String())

					// Retry logic (already correct in your snippet, kept for clarity)
					pendingTuple = &tuple
					continue
				}

				numProcessed++
				expectedDuration := time.Duration((numProcessed / r.InputRate) * float64(time.Second))
				targetTime := startTime.Add(expectedDuration)
				if targetTime.After(time.Now()) {
					time.Sleep(targetTime.Sub(time.Now()))
				}
			}
		}()

		// --- WAIT FOR APP COMPLETION ---
		<-appCompletedChan

		// 1. Stop Producers (Listeners and Input Reader)
		cancel()

		// 2. Wait for Producers to completely finish
		// This ensures the Listener has read every last byte from the workers
		// and pushed it into outputChan.
		wgProducers.Wait()

		// 3. Now that no one is writing, we can safely close the channel.
		close(outputChan)

		// 4. Wait for the Writer to finish flushing the remaining buffer.
		<-writerDone

		// 5. Cleanup Resources
		rpcWorkersLock.Lock()
		for _, worker := range rpcWorkers {
			_ = worker.Close()
		}
		rpcWorkersLock.Unlock()

		localOutputFile.Close()
		fmt.Println("RainStorm Application completed")
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
		app.LogFileChan <- fmt.Sprintf("Restarting Task because of Failure at VM: %s PID: %d op_exe: %s\n", app.TaskInformation[task.Stage][task.TaskNumber].Ip.String(), app.TaskInformation[task.Stage][task.TaskNumber].Pid, string(app.Ops[task.Stage].Name))
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
			//	remove a task from this stage
			app.Lock.Lock()
			app.LogFileChan <- fmt.Sprintf("Downscaling Stage: %d Rate: %.2f\n", args.Stage, args.Rate)
			app.removeTask(args.Stage)
			app.Lock.Unlock()
		} else if args.Rate > app.HighestRate {
			//	add a task to this stage
			app.Lock.Lock()
			taskNum := app.NextTaskNum[args.Stage]
			app.NextTaskNum[args.Stage]++
			app.LogFileChan <- fmt.Sprintf("Upscaling Stage: %d Rate: %.2f\n", args.Stage, args.Rate)
			app.addTask(args.Stage, taskNum)
			app.sendIps()
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
			//vm := splits[1]
			//pid, _ := strconv.Atoi(splits[2])
			//curApp.Lock.RLock()
			//for stageNum, stage := range curApp.TaskInformation {
			//	done := false
			//	for taskNum, info := range stage {
			//		if vm == info.Ip.String() && pid == info.Pid {
			//			rpcWorkersLock.RLock()
			//			worker := rpcWorkers[info.Ip.String()]
			//			var reply int
			//			_ = worker.Call("Worker.KillTask", TaskID{
			//				Task:  taskNum,
			//				Stage: stageNum,
			//			}, &reply)
			//			rpcWorkersLock.RUnlock()
			//			done = true
			//			break
			//		}
			//	}
			//	if done {
			//		break
			//	}
			//}
			//curApp.Lock.RUnlock()
			//break
			vm := splits[1]
			pid, _ := strconv.Atoi(splits[2])

			// We need to release the lock before calling ReceiveFailure to avoid Deadlock,
			// because ReceiveFailure acquires the lock itself.
			var foundTask bool
			var targetTask Task

			curApp.Lock.RLock()
			for stageNum, stage := range curApp.TaskInformation {
				for taskNum, info := range stage {
					if vm == info.Ip.String() && pid == info.Pid {
						targetTask = Task{
							TaskNumber: taskNum,
							Stage:      stageNum,
						}
						foundTask = true
						break
					}
				}
				if foundTask {
					break
				}
			}
			curApp.Lock.RUnlock()

			if foundTask {
				// 1. Kill the physical process on the worker
				rpcWorkersLock.RLock()
				worker, ok := rpcWorkers[vm] // Use the vm string to find the client
				rpcWorkersLock.RUnlock()

				if ok {
					var reply int
					err := worker.Call("Worker.KillTask", TaskID{
						Task:  targetTask.TaskNumber,
						Stage: targetTask.Stage,
					}, &reply)

					if err != nil {
						fmt.Println("Error killing task:", err)
					} else {
						fmt.Println("Task Killed via RPC.")
					}
				}

				// 2. IMPORTANT: Tell the Leader to recover/reschedule immediately
				fmt.Println("Triggering Leader Recovery...")
				var reply int
				// We call ReceiveFailure locally to update the map and send new IPs to everyone
				curApp.ReceiveFailure(targetTask, &reply)
			} else {
				fmt.Println("Task not found to kill.")
			}
			break

		case "list_tasks":
			//@TODO print local log file for task
			curApp.Lock.RLock()
			for stageNum, stage := range curApp.TaskInformation {
				for _, info := range stage {
					fmt.Printf("%s %d %s\n", info.Ip.String(), info.Pid, curApp.Ops[stageNum])
				}
			}
			curApp.Lock.RUnlock()
			break

		}
	}
}
