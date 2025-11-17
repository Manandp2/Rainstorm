package rpc_api

type GrepArgs struct {
	VMNumber string
	Args     []string
}

type GrepReply struct {
	LineCount int
	Output    []byte
	VMNumber  string
}

type GenerateLogArgs struct {
	VMNumber string
	Data     []byte
}
