package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	. "g14-mp3/mp1/resources/rpc-api"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
)

type GrepService struct{}

func main() {
	err := rpc.Register(new(GrepService))
	if err != nil {
		return
	}
	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		return
	}
	rpc.Accept(listener)
}

func (s *GrepService) Grep(args *GrepArgs, reply *GrepReply) error {
	if args == nil {
		return errors.New("GrepArgs must not be nil")
	}
	if reply == nil {
		return errors.New("GrepReply must not be nil")
	}
	cmd := exec.Command("grep", args.Args...)
	output, err := cmd.CombinedOutput()
	reply.VMNumber = args.VMNumber
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			reply.Output = nil
			return err
		}
		//if status code 1 or 2 --> no match found or no file found
		reply.LineCount = 0
		reply.Output = output
		return nil
	}
	// Get number matches, subtract 1 if last match ends with a '\n'
	reply.LineCount = len(bytes.Split(output, []byte{'\n'}))
	if output[len(output)-1] == '\n' {
		reply.LineCount--
	}
	reply.Output = output
	return nil
}

// GenerateLogs For testing purposes only
func (s *GrepService) GenerateLogs(args *GenerateLogArgs, reply *int) error {
	home, _ := os.UserHomeDir()
	fp := filepath.Join(home, fmt.Sprintf("vm%s.log", args.VMNumber))
	file, _ := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	defer func() {
		_ = file.Close()
	}()
	_, _ = file.Write(args.Data)

	writer := bufio.NewWriter(file)
	// write random chars
	for i := 0; i < 50000; i++ {
		_ = writer.WriteByte(byte(rand.Intn(126-33+1) + 33)) // random ascii char
		if i%100 == 0 {
			_, _ = writer.WriteString("\n")
		}
	}
	return nil
}
