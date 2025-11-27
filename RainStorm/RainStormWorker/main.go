package main

import (
	"fmt"
	"net"
	"net/rpc"
)

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
