package main

import (
	"bufio"
	"fmt"
	. "g14-mp4/RainStorm/resources"
	"io"
	"net"
	"net/rpc"
	"os/exec"
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

type taskOutput struct {
	stage  int
	output string
}
type Worker struct {
	done            chan bool
	tasksLocker     sync.Mutex
	tasks           map[Task]localTask
	ips             [][]net.IP
	taskOutputs     chan taskOutput
	connections     map[string]*WorkerClient
	stageOperations []Operation
}

type WorkerClient struct {
	conn net.Conn
	buf  *bufio.Reader
}

const clientTimeout = time.Second * 3
const ACK = "ACK"

func main() {
	leader, err := rpc.Dial("tcp", "fa25-cs425-1401.cs.illinois.edu:8020")
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
	for {
		server := rpc.NewServer()
		worker := Worker{
			done:        make(chan bool),
			tasks:       make(map[Task]localTask),
			taskOutputs: make(chan taskOutput, 100),
			connections: make(map[string]*WorkerClient),
		}
		err := server.Register(&worker)
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
				nextStage := out.stage + 1
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
					nextTask := HashString(key) % len(worker.ips[nextStage])
					nextWorker := worker.ips[nextStage][nextTask].String()
					client, ok := worker.connections[nextWorker]
					if !ok { // connect to a client for the first time
						conn, err := net.Dial("tcp", nextWorker+TuplePort)
						if err != nil {
							worker.taskOutputs <- out // just skip for now
							continue
						}
						client = &WorkerClient{
							conn: conn,
							buf:  bufio.NewReader(conn),
						}
						worker.connections[nextWorker] = client
					}

					// Send the tuple
					_ = client.conn.SetWriteDeadline(time.Now().Add(clientTimeout))
					_, err = fmt.Fprintf(client.conn, "%s\n", out.output)
					if err != nil { // Write didn't go through, disconnect and try again
						_ = client.conn.Close()
						delete(worker.connections, nextWorker)
						worker.taskOutputs <- out
						continue
					}

					// Wait for the ack
					_ = client.conn.SetReadDeadline(time.Now().Add(clientTimeout))
					ack, err := client.buf.ReadString('\n')
					if err != nil || ack != ACK {
						worker.taskOutputs <- out // didn't receive the ack, just try again
					}
				} else { // output data to the distributed file system
					// TODO: Write to HyDFS
				}
			}
		}()

		<-worker.done
		_ = leaderListener.Close()
		time.Sleep(1 * time.Second) // wait for os to release por	t 8021
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

func (w *Worker) ReceiveStages(stageOps []Operation, reply *int) error {
	w.stageOperations = stageOps
	return nil
}

func (w *Worker) ReceiveIPs(ips [][]net.IP, reply *int) error {
	w.ips = ips
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

	go func(pipe io.Reader, stage int, c chan<- taskOutput) {
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			c <- taskOutput{
				stage:  stage,
				output: scanner.Text(),
			}
		}
	}(taskStdout, t.Stage, w.taskOutputs)

	w.tasksLocker.Lock()
	w.tasks[t] = localTask{
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
	task, ok := w.tasks[t]
	if ok {
		_ = task.cmd.Process.Kill()
		_ = task.input.Close()
		_ = task.output.Close()
		go func(cmd *exec.Cmd) {
			_ = cmd.Wait()
		}(task.cmd)
		delete(w.tasks, t)
	}
	return nil
}
