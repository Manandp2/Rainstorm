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
	Ips                      [][]net.IP // [stage][task]
	NextAvailableVM          int
	Lock                     sync.Mutex //@TODO: add locks when accessing the rainstorm object
}

var workers WorkerIps
var numWorkers int
var numSuccessfulDials int
var rpcWorkers map[string]*rpc.Client
var rpcWorkersLock sync.RWMutex

func main() {
	workers = WorkerIps{}
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
		r.Ips = make([][]net.IP, r.NumStages)
		//r.tasksAtWorker = make(map[string][]resources.Task) //consider adding a lock for this b/c of Global RM
		workers.l.Lock()
		numWorkers = len(workers.ips)
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
		rpcWorkersLock.Unlock()

		r.NextAvailableVM = 0
		for i := range r.NumStages {
			r.Ips[i] = make([]net.IP, r.NumTasksPerStage)
			for j := range r.NumTasksPerStage {
				r.addTask(i, j)
			}
		}
		workers.l.Unlock()
		r.sendIps()
		r.sendOpNames()

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

		// CLEANUP: do once the current RainStorm application is done
		for _, worker := range rpcWorkers {
			_ = worker.Close()
		}

		err = globalRmListener.Close()
		if err != nil {
			fmt.Println(err)
		}
	}

}

func (app *RainStorm) ReceiveRateUpdate(args RmUpdate, reply *int) error {
	if app.AutoScale {
		if args.Rate < app.LowestRate {
			//	add a task in this stage
			workers.l.Lock()
			app.addTask(args.Stage, len(app.Ips[args.Stage]))
			workers.l.Unlock()
			app.sendIps()
		} else if args.Rate > app.HighestRate {
			//	remove a task in this stage
			app.removeTask(args.Stage, len(app.Ips[args.Stage]))
		}
	}
	return nil
}

func (app *RainStorm) sendIps() {
	waitingChan := make(chan *rpc.Call, len(rpcWorkers))
	numSuccess := 0
	for _, worker := range rpcWorkers {
		var reply int
		worker.Go("Worker.ReceiveIPs", app.Ips, &reply, waitingChan)
		numSuccess++
	}
	for i := 0; i < numSuccess; i++ {
		x := <-waitingChan
		if x.Error != nil {
			fmt.Println("Failed to send IPs to workers: " + x.Error.Error())
		}
	}
}

func (app *RainStorm) sendOpNames() {
	waitingChan := make(chan *rpc.Call, len(rpcWorkers))
	numSuccess := 0
	for _, worker := range rpcWorkers {
		var reply int
		worker.Go("Worker.ReceiveOpNames", app.Ops, &reply, waitingChan)
		numSuccess++
	}
	for i := 0; i < numSuccess; i++ {
		x := <-waitingChan
		if x.Error != nil {
			fmt.Println("Failed to send list of operations to workers: " + x.Error.Error())
		}
	}
}

func (app *RainStorm) addTask(stageNum int, taskNum int) { //MUST BE WRAPPED IN LOCK WHEN CALLED
	if taskNum >= len(app.Ips[stageNum]) {
		app.Ips[stageNum] = append(app.Ips[stageNum], workers.ips[app.NextAvailableVM%numWorkers])
	} else {
		app.Ips[stageNum][taskNum] = workers.ips[app.NextAvailableVM%numWorkers]
	}
	task := Task{
		TaskNumber: taskNum,
		Stage:      stageNum,
		Executable: app.Ops[stageNum],
	}
	app.NextAvailableVM++

	var reply int
	rpcWorkersLock.Lock()
	rpcWorker := rpcWorkers[app.Ips[stageNum][taskNum].String()]
	rpcWorkersLock.Unlock()
	err := rpcWorker.Call("Worker.AddTask", task, &reply)
	if err != nil {
		fmt.Println("Failed to send request to add task: " + err.Error())
	}
}

func (app *RainStorm) removeTask(stageNum int, taskNum int) {
	deletedTaskIp := app.Ips[stageNum][len(app.Ips[stageNum])-1]
	if len(app.Ips[stageNum]) != 0 {
		app.Ips[stageNum] = app.Ips[stageNum][:len(app.Ips[stageNum])-1] //go garbage collector will clean up the last element
	}

	app.sendIps()

	task := Task{
		TaskNumber: taskNum,
		Stage:      stageNum,
		Executable: app.Ops[stageNum],
	}
	var reply int
	rpcWorkersLock.Lock()
	rpcWorker := rpcWorkers[deletedTaskIp.String()]
	rpcWorkersLock.Unlock()
	err := rpcWorker.Call("Worker.KillTask", task, &reply)
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
