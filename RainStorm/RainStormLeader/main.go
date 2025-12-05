package main

import (
	"bufio"
	"fmt"
	. "g14-mp4/RainStorm/resources"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WorkerIps struct {
	ips []net.IP
	l   sync.Mutex
}

type RainStorm struct {
	NumStages                int
	NumTasksPerStage         int
	HydfsSrcDirectory        string
	HydfsDestinationFileName string
	ExactlyOnce              bool
	AutoScale                bool
	InputRate                int
	LowestRate               int
	HighestRate              int
	Ops                      []Operation
	Ips                      []map[int]net.IP // [stage][task] --> IP
	//TaskCompletion           []CompletionTuple // [stage][task] --> {Counter, map[task] --> bool of if finished}
	NextTaskNum     []int // [stage]
	NextAvailableVM int
	Lock            *sync.Mutex //@TODO: add locks when accessing the rainstorm object
}

var workers WorkerIps
var numWorkers int
var numSuccessfulDials int
var rpcWorkers map[string]*rpc.Client
var rpcWorkersLock sync.RWMutex
var appCompletedChan chan bool

func main() {
	workers = WorkerIps{}
	appCompletedChan = make(chan bool, 1)
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

	for {
		r := <-input
		// INITIATE NEW RAINSTORM APPLICATION
		//@TODO: Make AddWorker function
		workers.l.Lock()
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
		numWorkers = len(workers.ips)
		rpcWorkersLock.Unlock()

		r.Lock = new(sync.Mutex)
		r.Lock.Lock()
		r.Ips = make([]map[int]net.IP, r.NumStages)
		r.NextTaskNum = make([]int, r.NumStages)
		//r.TaskCompletion = make([]CompletionTuple, r.NumStages)
		r.initWorker()
		r.NextAvailableVM = 0
		for i := range r.NumStages {
			r.Ips[i] = make(map[int]net.IP)
			//r.TaskCompletion[i] = CompletionTuple{
			//	Counter:      0,
			//	StateTracker: make(map[int]bool),
			//}
			for _ = range r.NumTasksPerStage {
				r.addTask(i)
			}
		}
		workers.l.Unlock()
		r.sendIps()
		r.Lock.Unlock()

		//Global RM
		/*
			1. open listener for current task input rates from workers
			2. check if autoscale is on, if it is ->
			3. compare rates to see if changes are needed
			4. complete changes
		*/
		appServer := rpc.NewServer()
		err := appServer.Register(r)
		if err != nil {
			fmt.Println(err)
			continue
		}
		globalRmListener, err := net.Listen("tcp", GlobalRMPort)
		if err != nil {
			fmt.Println("GlobalRM unable to connect to worker: " + err.Error())
			continue
		}
		go appServer.Accept(globalRmListener)

		// @TODO: needs to wait for the application to complete before cleaning up --> come back to this
		<-appCompletedChan //blocking
		println("RainStorm Application completed!")

		// CLEANUP: do once the current RainStorm application is done
		workers.l.Lock()
		for _, worker := range rpcWorkers {
			_ = worker.Close()
		}
		workers.l.Unlock()

		err = globalRmListener.Close()
		if err != nil {
			fmt.Println(err)
		}
	}

}

//@TODO: upon failure, the worker needs to reboot the task with the same taskID

func (app *RainStorm) ReceiveRateUpdate(args RmUpdate, reply *int) error {
	app.Lock.Lock()
	defer app.Lock.Unlock()
	if app.AutoScale {
		if args.Rate < app.LowestRate {
			//	add a task to this stage
			workers.l.Lock()
			app.addTask(args.Stage)
			workers.l.Unlock()
			app.sendIps()
		} else if args.Rate > app.HighestRate {
			//	remove a task from this stage
			app.removeTask(args.Stage)
		}
	}
	return nil
}

func (app *RainStorm) ReceiveTaskCompletion(args Task, reply *int) error {
	//stage completion manager --> manage markers from tasks saying they are done
	app.Lock.Lock()
	defer app.Lock.Unlock()
	if _, exists := app.Ips[args.Stage][args.TaskNumber]; exists {
		delete(app.Ips[args.Stage], args.TaskNumber)
		//app.CurNumTasks[args.Stage] -= 1
		app.sendIps()
		if len(app.Ips[args.Stage]) == 0 {
			// stage completed
			if args.Stage+1 < app.NumStages {
				app.sendStageCompletion(args.Stage + 1)
			} else {
				appCompletedChan <- true
			}
		}

	} else {
		//do nothing because this should never happen
		fmt.Printf("Received task completion for: %d, BUT should not have received this\n", args.TaskNumber)
	}
	return nil
}

func (app *RainStorm) sendStageCompletion(sendingStage int) { //MUST BE WRAPPED IN APP LOCK WHEN CALLED
	// @TODO: send out "EOF" to stage when previous stage is done

}

func (app *RainStorm) sendIps() { // MUST BE CALLED INSIDE RAINSTORM LOCK --> only called when current app is modified
	waitingChan := make(chan *rpc.Call, len(rpcWorkers))
	numSuccess := 0
	rpcWorkersLock.RLock()
	for _, worker := range rpcWorkers {
		var reply int
		worker.Go("Worker.ReceiveIPs", app.Ips, &reply, waitingChan)
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
		Time:          time.Now(),
		HyDFSDestFile: app.HydfsDestinationFileName,
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

func (app *RainStorm) addTask(stageNum int) { //MUST BE WRAPPED IN LOCK WHEN CALLED
	//if taskNum > app.StageCounter[stageNum]) {
	//	app.Ips[stageNum] = append(app.Ips[stageNum], workers.ips[app.NextAvailableVM%numWorkers])
	//} else {
	//	app.Ips[stageNum][taskNum] = workers.ips[app.NextAvailableVM%numWorkers]
	//}
	taskNum := app.NextTaskNum[stageNum]
	app.Ips[stageNum][taskNum] = workers.ips[app.NextAvailableVM%numWorkers]
	//app.TaskCompletion[stageNum].StateTracker[taskNum] = false
	app.NextTaskNum[stageNum]++
	app.NextAvailableVM++
	task := Task{
		TaskNumber: taskNum,
		Stage:      stageNum,
		Executable: app.Ops[stageNum],
	}

	var reply int
	rpcWorkersLock.RLock()
	rpcWorker := rpcWorkers[app.Ips[stageNum][taskNum].String()]
	rpcWorkersLock.RUnlock()
	err := rpcWorker.Call("Worker.AddTask", task, &reply)
	if err != nil {
		fmt.Println("Failed to send request to add task: " + err.Error())
	}
}

func (app *RainStorm) removeTask(stageNum int) { //MUST BE WRAPPED IN APP LOCK WHEN CALLED
	if len(app.Ips[stageNum]) <= 1 { // only 1 task remaining in the stage
		return
	}
	var taskNum int
	for k := range app.Ips[stageNum] {
		// getting first taskNum when iterating to remove
		taskNum = k
		break
	}

	deletedTaskIp, exists := app.Ips[stageNum][taskNum]
	if !exists {
		fmt.Printf("Failed to remove task: %d, stage %d: not exists", taskNum, stageNum)
		return
	}

	delete(app.Ips[stageNum], taskNum)
	app.sendIps()

	task := Task{
		TaskNumber: taskNum,
		Stage:      stageNum,
		Executable: app.Ops[stageNum],
	}
	var reply int
	rpcWorkersLock.RLock()
	rpcWorker := rpcWorkers[deletedTaskIp.String()]
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
					rainStorm.InputRate, err = strconv.Atoi(splits[i])
					if err != nil {
						fmt.Println("Failed to parse InputRate: " + err.Error())
						bad = true
					}
					break
				case i == len(splits)-2: // LowestRate
					rainStorm.LowestRate, err = strconv.Atoi(splits[i])
					if err != nil {
						fmt.Println("Failed to parse LowestRate: " + err.Error())
						bad = true
					}
					break
				case i == len(splits)-1: // HighestRate
					rainStorm.HighestRate, err = strconv.Atoi(splits[i])
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
			break

		case "list_tasks":
			break

		}
	}
}
