package resources

import (
	"bufio"
	"net"
	"time"
)

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

type RmUpdate struct {
	Stage int
	Rate  float64
	Task  int
}

type InitArgs struct {
	Ops           []Operation
	Time          time.Time
	HyDFSDestFile string
	LowWatermark  float64
	HighWatermark float64
}

type WorkerClient struct {
	Conn net.Conn
	Buf  *bufio.Reader
}
