package resources

import "net"

const IntroducePort = ":8020"
const AssignmentPort = ":8021"
const TuplePort = ":8022"
const GlobalRMPort = ":8023"
const AckPort = ":8024"

type OperationName string

const (
	Transform      OperationName = "Operation1"
	Filter         OperationName = "Operation2"
	AggregateByKey OperationName = "Operation3"
)

type Operation struct {
	Name OperationName
	Args string
}

type Task struct {
	TaskNumber int
	Stage      int
	Executable Operation
}

type WorkerInfo struct {
	Task     Task
	Ips      [][]net.IP
	StageOps []string
}

type RmUpdate struct {
	Stage int
	Rate  int
	Task  int
}
