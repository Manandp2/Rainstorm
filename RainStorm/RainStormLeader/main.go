package main

import (
	"bufio"
	"fmt"
	"g14-mp4/RainStorm/resources"
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

func main() {
	workers := WorkerIps{}
	go func() {
		err := rpc.Register(&workers)
		if err != nil {
			fmt.Println(err)
			return
		}
		listener, err := net.Listen("tcp", ":8020")
		if err != nil {
			fmt.Println(err)
			return
		}
		rpc.Accept(listener)
	}()
	input := make(chan resources.RainStorm)
	go processStdin(input)
	for {
		r := <-input
		r.Ips = make([][]net.IP, r.NumStages)
		workers.l.Lock()
		numWorkers := len(workers.ips)
		currentVM := 0
		for i := range r.NumStages {
			r.Ips[i] = make([]net.IP, r.NumTasksPerStage)
			for j := range r.NumTasksPerStage {
				r.Ips[i][j] = workers.ips[currentVM%numWorkers]
				currentVM++
			}
		}
		workers.l.Unlock()

		for i := range r.NumStages {
			for j := range r.NumTasksPerStage {
				r.Ips[i][j] = workers.ips[currentVM%numWorkers]

			}
		}
	}

}

func processStdin(i1 chan<- resources.RainStorm) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		scanner.Scan()
		stdin := scanner.Text()
		stdin = strings.TrimSpace(stdin)
		splits := strings.Split(stdin, " ")
		switch splits[0] {
		case "RainStorm": //
			var rainStorm resources.RainStorm
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
					rainStorm.Ops = append(rainStorm.Ops, resources.Operation{Name: splits[i], Args: splits[i+1]})
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

func (w *WorkerIps) AddWorker(args net.IP, reply *int) error {
	w.l.Lock()
	w.ips = append(w.ips, args)
	w.l.Unlock()
	return nil
}
