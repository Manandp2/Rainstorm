package resources

import "net"

type Operation struct {
	Name string
	Args string
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
}

type Task struct {
	TaskNumber int
	Stage      int
	Executable Operation
}
