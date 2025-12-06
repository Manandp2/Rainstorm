package failureDetector

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math/rand/v2"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type NodeId struct {
	Time int64
	Ip   uint32
	Port uint16
}

func (i NodeId) IP() string {
	ipAddr := make([]byte, 4)
	binary.BigEndian.PutUint32(ipAddr, i.Ip)
	return fmt.Sprintf("%d.%d.%d.%d", ipAddr[0], ipAddr[1], ipAddr[2], ipAddr[3])
}

func (i NodeId) Address() string {
	ipAddr := make([]byte, 4)
	binary.BigEndian.PutUint32(ipAddr, i.Ip)
	return fmt.Sprintf("%d.%d.%d.%d:%d", ipAddr[0], ipAddr[1], ipAddr[2], ipAddr[3], i.Port)
}

func (i NodeId) String() string {
	return fmt.Sprintf("%s@%d", i.Address(), i.Time)
}

type Suspicion struct {
	Incarnation uint32
	Suspected   bool
}

func (s Suspicion) String() string {
	return fmt.Sprintf("Incarnation:%03d   |   Suspected:%t", s.Incarnation, s.Suspected)
}

type Membership struct {
	Node            NodeId
	Suspicion       Suspicion
	Failed          bool
	Leaving         bool
	HeartBeatNumber uint32
}

func (m Membership) String() string {
	return fmt.Sprintf("Node:%s    |    HeartBeatNumber:%010d   |   %s   |   Failed:%t", m.Node, m.HeartBeatNumber, m.Suspicion, m.Failed)
}

type Packet struct {
	Memberships []Membership
	IsPing      bool
}

type Ack struct {
	packet Packet
	addr   net.Addr
}

type networkInput struct {
	packet []byte
	addr   net.Addr
}

type IntroducerReply struct {
	Memberships []Membership
	IsGossip    bool
	SuspicionOn bool
}
type localMembership struct {
	membership  Membership
	lastUpdated time.Time
}

func (m localMembership) String() string {
	return fmt.Sprintf("%s   |   lastUpdated:%d", m.membership, m.lastUpdated.UnixMilli())
}

type Detector struct {
	// Stuff for the membership list
	lock sync.RWMutex
	list map[NodeId]localMembership

	logFile *os.File
	// Stuff for detecting failures
	heartBeatCounter uint32
	MyNode           NodeId
	config           struct {
		suspicionOn bool
		isGossip    bool
	}

	listener      *net.UDPConn
	beatingTicker *time.Ticker

	leaveChan       chan struct{}
	joinChan        chan struct{}
	doneLeavingChan chan struct{}

	AddMe    chan NodeId
	RemoveMe chan NodeId
}

const gossipFrequency = time.Millisecond * 500
const pingFrequency = time.Millisecond * 250
const failureTime = time.Second*1 + time.Millisecond*300

func (d *Detector) Init() {
	hd, _ := os.UserHomeDir()
	d.logFile, _ = os.OpenFile(filepath.Join(hd, "failureDetector.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	d.list = make(map[NodeId]localMembership)
	// Register RPCs
	err := rpc.Register(d)
	if err != nil {
		d.logFatal(err)
	}
	// VM 1 is always the introducer
	go func() {
		rpcListener, err := net.Listen("tcp", ":8002")
		if err != nil {
			d.logFatal(err)
		}
		rpc.Accept(rpcListener)
	}()
	d.heartBeatCounter = 0
	d.config.isGossip = true
	d.config.suspicionOn = true
	d.beatingTicker = time.NewTicker(pingFrequency)

	d.leaveChan = make(chan struct{})
	d.doneLeavingChan = make(chan struct{})
	d.joinChan = make(chan struct{})
	d.AddMe = make(chan NodeId, 10)
	d.RemoveMe = make(chan NodeId, 10)
	d.introduceMyself()
	d.AddMe <- d.MyNode
}

func (d *Detector) Start() {
	defer func() {
		_ = d.logFile.Close()
	}()

	ackChan := make(chan Ack)
	go func() {
		var remoteAddr net.Addr
		chooseNode := func() NodeId { // Chooses a random node to ping
			d.lock.RLock()
			defer d.lock.RUnlock()

			var potentialTargets []NodeId
			for node, m := range d.list {
				if node != d.MyNode && !m.membership.Failed && !m.membership.Leaving {
					potentialTargets = append(potentialTargets, node)
				}
			}

			if len(potentialTargets) == 0 {
				return NodeId{}
			}
			return potentialTargets[rand.IntN(len(potentialTargets))]
		}
		sendHeartbeat := func() {
			const fanOut = 2
			for i := 0; i < fanOut; i++ {
				target := chooseNode()
				if target.Ip != 0 {
					d.sendPacketToNode(target)
				}
			}
		}
		sendPing := func() NodeId {
			targetNode := chooseNode()
			if targetNode.Ip != 0 {
				remoteAddr = d.sendPacketToNode(targetNode)
			}
			return targetNode
		}

		for {
			select {
			case <-d.beatingTicker.C:
				if d.config.isGossip {
					sendHeartbeat()
				} else {
					sentNode := sendPing()
					if sentNode.Ip == 0 { // didn't send to anyone, skip
						continue
					}
					timeout := time.After(pingFrequency)
					waiting := true
					for waiting {
						select {
						case myAck := <-ackChan: // received ack
							expected := strings.Split(remoteAddr.String(), ":")[0]
							received := strings.Split(myAck.addr.String(), ":")[0]
							if received == expected {
								d.updateMembershipList(&myAck.packet.Memberships)
								waiting = false
							}
							d.lock.Lock()
							l := d.list[sentNode]
							l.lastUpdated = time.Now()
							d.list[sentNode] = l
							d.lock.Unlock()
							break
						case <-timeout: // did not receive ack in time; past T_fail
							d.lock.Lock()
							waiting = false
							if d.config.suspicionOn {
								l := d.list[sentNode]
								if l.membership.Suspicion.Suspected {
									l.membership.Failed = true
									d.writeToLogAndStdout(fmt.Sprintf("FAILURE because no ack from %s at time %s\n", sentNode, time.Now().Format("03:04:05PM")))
									d.RemoveMe <- sentNode
								} else {
									l.membership.Suspicion.Suspected = true
									d.writeToLogAndStdout(fmt.Sprintf("SUSPECTED because no ack from %s at time %s\n", sentNode, time.Now().Format("03:04:05PM")))
								}
								d.list[sentNode] = l
							} else {
								l := d.list[sentNode]
								l.membership.Failed = true
								d.list[sentNode] = l
								d.writeToLogAndStdout(fmt.Sprintf("FAILURE because no ack from %s at time %s\n", sentNode, time.Now().Format("03:04:05PM")))
								d.RemoveMe <- sentNode
							}
							d.lock.Unlock()
							break
						}
					}
				}
			case <-d.leaveChan:
				if d.config.isGossip {
					sendHeartbeat() // send saying we are leaving
				} else { //PingAck
					received := false
					for !received {
						sentNode := sendPing()
						if sentNode.Ip == 0 { // need to try sending to a different person
							continue
						}
						timeout := time.After(pingFrequency)
						waiting := true
						// need to ensure the ping received out with us marked as leaving is received by a node
						for waiting {
							select {
							case myAck := <-ackChan:
								if strings.Split(remoteAddr.String(), ":")[0] == strings.Split(myAck.addr.String(), ":")[0] {
									waiting = false
									received = true
									fmt.Println("we got the ack, can leave")
								}
								break
							case <-timeout: //need to ping a node again
								waiting = false
								fmt.Println("we didn't get the ack, try again")
								break
							}
						}
					}
				}
				d.doneLeavingChan <- struct{}{} // we are ready to rejoin
				// wait for a join flag
				<-d.joinChan
			}
		}
	}()

	// go routine to check every T_fail for suspect or failing nodes FOR GOSSIP; T_cleanup for both protocols
	go func() {
		//check every second
		ticker := time.NewTicker(failureTime)
		for {
			select {
			case <-ticker.C:
				d.lock.Lock()
				for node, m := range d.list {
					if node == d.MyNode {
						continue
					}
					if d.config.suspicionOn {
						if m.membership.Failed {
							// The only thing to check here is T_cleanup
							if m.lastUpdated.Add(failureTime * 5).Before(time.Now()) {
								delete(d.list, node)
								d.writeToLogAndStdout(fmt.Sprintf("DELETING after T_cleanup %s\n", node))
							}
						} else if m.membership.Leaving {
							// the only thing to check here is T_cleanup
							if m.lastUpdated.Add(failureTime * 4).Before(time.Now()) {
								delete(d.list, node)
								d.writeToLogAndStdout(fmt.Sprintf("DELETING after T_cleanup %s\n", node))
							}
						} else if d.config.isGossip && m.membership.Suspicion.Suspected {
							// Check if the suspicion time is past T_fail
							if m.lastUpdated.Add(failureTime * 2).Before(time.Now()) {
								m.membership.Failed = true
								d.list[node] = m
								d.writeToLogAndStdout(fmt.Sprintf("FAILURE IN LOCAL CHECK : %s at time %s \n", node, time.Now().Format("03:04:05PM")))
								d.RemoveMe <- node
							}
						} else if d.config.isGossip && m.lastUpdated.Add(failureTime).Before(time.Now()) {
							// not suspected, leaving, or failed, but has exceeded T_fail --> mark as suspected
							m.membership.Suspicion.Suspected = true
							d.list[node] = m
							d.writeToLogAndStdout(fmt.Sprintf("SUSPECTED : %s at time %s \n", node, time.Now().Format("03:04:05PM")))
						}
					} else { // suspicion off
						if m.membership.Failed || m.membership.Leaving {
							// The only thing to check here is T_cleanup
							if m.lastUpdated.Add(failureTime * 4).Before(time.Now()) {
								delete(d.list, node)
								d.writeToLogAndStdout(fmt.Sprintf("DELETING after T_cleanup %s\n", node))
							}
						} else if d.config.isGossip && m.lastUpdated.Add(failureTime*2).Before(time.Now()) {
							// not leaving, or failed, but has exceeded T_fail --> mark as failed
							m.membership.Failed = true
							d.list[node] = m
							d.writeToLogAndStdout(fmt.Sprintf("LOCAL FAILURE : %s at %s\n", m.membership.Node, time.Now().Format("03:04:05PM")))
							d.RemoveMe <- node
						}
					}
				}
				d.lock.Unlock()
			case <-d.leaveChan:
				// wait for a join flag
				d.doneLeavingChan <- struct{}{} // we are ready to rejoin
				<-d.joinChan
			}
		}
	}()

	// go routine to listen for heartbeats
	go func() {
		networkChan := make(chan networkInput)
		networkListener := func() {
			for {
				buf := make([]byte, 2048)
				n, addr, err := d.listener.ReadFrom(buf)
				if err != nil { // listener will get closed on 'leave' command, so continue to go to leaveChan case
					return
				}
				// Introduce drop rate here if we want
				const dropRate = 0 // percent from 0-100 of packets to drop
				if rand.IntN(100) >= dropRate {
					networkChan <- networkInput{buf[:n], addr}
				}
			}
		}
		go networkListener() // initial listening
		for {
			select {
			// listen for a leave flag
			case <-d.leaveChan:
				// need to send the ack on our last ping
				if !d.config.isGossip {
					input := <-networkChan
					var packet Packet
					if err := gob.NewDecoder(bytes.NewReader(input.packet)).Decode(&packet); err != nil {
						d.logFatal(err)
					}
					ackChan <- Ack{packet: packet, addr: input.addr}
				}
				d.doneLeavingChan <- struct{}{} // we are ready to rejoin
				// wait for a join flag
				<-d.joinChan
				go networkListener() // restart listening when rejoining
			case input := <-networkChan: // wait for a heartbeat, ping, or ack
				var packet Packet
				if err := gob.NewDecoder(bytes.NewReader(input.packet)).Decode(&packet); err != nil {
					d.logFatal(err)
				}
				if !d.config.isGossip { //PingAck mode
					if packet.IsPing { // need to send ack back
						d.updateMembershipList(&packet.Memberships)
						go func(addr net.Addr) { // send the ack
							s := strings.Split(addr.String(), ":")[0] + ":8001"
							r, _ := net.ResolveUDPAddr("udp", s)
							conn, err := net.DialUDP("udp", nil, r)
							if err != nil {
								return
							}
							var dToSend []Membership
							d.lock.RLock()
							for _, m := range d.list {
								if m.membership.Node.Ip == 0 {
									d.writeToLogAndStdout(fmt.Sprintf("INVALID_NODE in local list(size=%d) when sending at %s\n", len(d.list), time.Now().Format("03:04:05PM")))
								}
								dToSend = append(dToSend, m.membership)
							}
							d.lock.RUnlock()
							var buf bytes.Buffer
							if err := gob.NewEncoder(&buf).Encode(Packet{dToSend, false}); err != nil {
								d.logFatal(fmt.Errorf("failed to encode: %w", err))
							}
							_, _ = conn.Write(buf.Bytes())
							_ = conn.Close()
						}(input.addr)
					} else { // received an ack pass this to the sending goroutine
						ackChan <- Ack{packet, input.addr}
					}
				} else {
					d.updateMembershipList(&packet.Memberships)
				}
			}
		}
	}()
}

// getOutboundIP Get preferred outbound Ip of this machine, source: https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
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

// IntroduceNode introduces a Node to the network
func (d *Detector) IntroduceNode(args *Membership, reply *IntroducerReply) error {
	/*
		1.Receive a membership from a Node (happens when func is called)
		2.Update the local localMembership list by adding the new Node
		3.Send the current membership list, and protocol back to the new Node
	*/

	//step 2
	d.lock.Lock()
	defer d.lock.Unlock()
	d.list[args.Node] = localMembership{*args, time.Now()}
	d.AddMe <- args.Node
	d.writeToLogAndStdout(fmt.Sprintf("JOINED: %s\n", args.Node))

	//step 3
	for _, m := range d.list {
		reply.Memberships = append(reply.Memberships, m.membership)
	}
	reply.IsGossip = d.config.isGossip
	reply.SuspicionOn = d.config.suspicionOn
	return nil
}

// updateMembershipList updates the local membership list based on received remote membership data.
// It handles nodes joining, leaving, failing, and suspicion changes.
func (d *Detector) updateMembershipList(memberships *[]Membership) {
	d.lock.Lock()
	for _, remote := range *memberships {
		local, exists := d.list[remote.Node]
		if remote.Node == d.MyNode {
			if remote.Suspicion.Suspected {
				local.membership.Suspicion.Incarnation += 1
				d.list[d.MyNode] = local
			}
			continue
		}
		if exists {
			if remote.Leaving {
				if !local.membership.Leaving {
					local.membership.Leaving = true
					if d.config.isGossip && remote.HeartBeatNumber > local.membership.HeartBeatNumber {
						local.membership.HeartBeatNumber = remote.HeartBeatNumber
					}
					local.lastUpdated = time.Now()
					d.list[remote.Node] = local
					_, _ = d.logFile.WriteString(fmt.Sprintf("LEAVING : %s at %s\n", local.membership.Node, time.Now().Format("03:04:05PM")))
				}
			} else if !local.membership.Failed && !local.membership.Leaving {
				// locally not marked failed or leaving, but I am being told that this node has failed
				if remote.Failed {
					local.membership.Failed = true
					local.lastUpdated = time.Now().Add(-failureTime * 2) // Update for T_cleanup, if it failed locally, at this point we would be 2 T_fails past the updated time
					d.writeToLogAndStdout(fmt.Sprintf("FAILURE : %s at %s\n", local.membership.Node, time.Now().Format("03:04:05PM")))
					d.list[remote.Node] = local
					d.RemoveMe <- remote.Node
					continue
				}
				if d.config.suspicionOn {
					if remote.Suspicion.Incarnation > local.membership.Suspicion.Incarnation { // #Inc overrides lower #Inc
						local.membership.Suspicion = remote.Suspicion
						if d.config.isGossip {
							local.membership.HeartBeatNumber = remote.HeartBeatNumber
						}
						if remote.Suspicion.Suspected {
							d.writeToLogAndStdout(fmt.Sprintf("SUSPECTED : %s at time %s \n", local.membership.Node, time.Now().Format("03:04:05PM")))
							local.lastUpdated = time.Now().Add(-failureTime) // Update for T_fail, if we suspected locally, at this point we would be 1 T_fails past the updated time
						} else {
							local.lastUpdated = time.Now()
						}
						d.list[remote.Node] = local
						continue
					} else if remote.Suspicion.Incarnation == local.membership.Suspicion.Incarnation && remote.Suspicion.Suspected && !local.membership.Suspicion.Suspected {
						// Within Inc#, Sus overrides healthy
						local.membership.Suspicion.Suspected = remote.Suspicion.Suspected
						d.writeToLogAndStdout(fmt.Sprintf("SUSPECTED : %s at time %s \n", local.membership.Node, time.Now().Format("03:04:05PM")))
						local.lastUpdated = time.Now().Add(-failureTime) // Update for T_fail, if we suspected locally, at this point we would be 1 T_fails past the updated time
						d.list[remote.Node] = local
						continue
					}
				}
				if d.config.isGossip && remote.HeartBeatNumber > local.membership.HeartBeatNumber {
					local.membership.HeartBeatNumber = remote.HeartBeatNumber
					local.lastUpdated = time.Now()
					d.list[remote.Node] = local
				}

			}
		} else if !remote.Leaving && !remote.Failed { //adding a valid node that is not in my membership list
			d.writeToLogAndStdout(fmt.Sprintf("JOINED: %s\n", remote.Node))
			if d.config.suspicionOn && remote.Suspicion.Suspected {
				d.writeToLogAndStdout(fmt.Sprintf("SUSPECTED : %s at time %s \n", local.membership.Node, time.Now().Format("03:04:05PM")))
				d.list[remote.Node] = localMembership{remote, time.Now().Add(-failureTime)}
			} else {
				d.list[remote.Node] = localMembership{remote, time.Now()}
			}
			d.AddMe <- remote.Node
		}
	}
	d.lock.Unlock()
}

// sends a packet to a selected node
func (d *Detector) sendPacketToNode(node NodeId) net.Addr {
	d.lock.Lock()
	receiverAddr, err := net.ResolveUDPAddr("udp", node.Address())
	if err != nil {
		d.logFatal(fmt.Errorf("failed to resolve UDP address %w", err))
	}

	conn, err := net.DialUDP("udp", nil, receiverAddr)
	if err != nil {
		return nil
	}
	defer func() {
		_ = conn.Close()
	}()
	// increment heartbeat
	if d.config.isGossip {
		d.heartBeatCounter++
		myMembership := d.list[d.MyNode]
		myMembership.membership.HeartBeatNumber = d.heartBeatCounter
		myMembership.lastUpdated = time.Now()
		d.list[d.MyNode] = myMembership
	}

	var dToSend []Membership
	for _, m := range d.list {
		dToSend = append(dToSend, m.membership)
	}
	d.lock.Unlock()
	// encode the list into the local buffer first, then send it over udp
	// Idea to put gob data into the local buffer first: https://groups.google.com/g/golang-nuts/c/EuJaS7KARYc?pli=1
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(Packet{dToSend, !d.config.isGossip}); err != nil {
		d.logFatal(fmt.Errorf("FAILED to encode: %w", err))
	}
	_, _ = conn.Write(buf.Bytes())
	return conn.RemoteAddr()
}

func (d *Detector) logFatal(err error) {
	_, _ = d.logFile.WriteString(fmt.Sprintf("ERROR: %s\n", err))
	panic(err)
}

func (d *Detector) introduceMyself() {
	// Start up the heartbeat counter and set up the localMembership list
	d.MyNode = NodeId{time.Now().UnixMilli(), binary.BigEndian.Uint32(getOutboundIP()), 8001}
	d.list[d.MyNode] = localMembership{
		membership: Membership{
			Node:            d.MyNode,
			HeartBeatNumber: d.heartBeatCounter,
			Suspicion: Suspicion{
				Incarnation: 0,
				Suspected:   false,
			},
			Failed:  false,
			Leaving: false,
		},
		lastUpdated: time.Now(),
	}

	// Open the port for failure listening, if this fails no point to join
	addr, err := net.ResolveUDPAddr("udp", ":8001")
	d.listener, err = net.ListenUDP("udp", addr)
	if err != nil {
		d.logFatal(err)
	}

	if name, _ := os.Hostname(); name != "fa25-cs425-1401.cs.illinois.edu" {
		// hello introducer
		server, err := rpc.Dial("tcp", "fa25-cs425-1401.cs.illinois.edu:8002")
		if err != nil {
			d.logFatal(err)
		}
		var reply IntroducerReply //contains the introducer's membership list and the network's protocol
		myMembership := d.list[d.MyNode].membership
		err = server.Call("Detector.IntroduceNode", &myMembership, &reply)
		if err != nil {
			d.logFatal(fmt.Errorf("rpc error: %w", err))
		}

		//assign network protocol to local protocol
		d.config.isGossip = reply.IsGossip
		d.config.suspicionOn = reply.SuspicionOn
		d.updateMembershipList(&reply.Memberships)
	}
}

// SwitchSuspicion switches the suspicion mode.
// When args = true, turn on suspicion; when args = false, turn off suspicion
func (d *Detector) SwitchSuspicion(args *bool, reply *bool) error {
	if !*args && d.config.suspicionOn { // If suspicion is turning off, unsuspect everything
		d.lock.Lock()
		for n, m := range d.list {
			m.membership.Suspicion.Suspected = false
			d.list[n] = m
		}
		d.lock.Unlock()
	}
	d.config.suspicionOn = *args // Change the variable on our node to reflect the remote
	return nil
}

// SwitchMode switches the direction mode.
// When *args = true, use gossip; when *args = false, use ping/ack
func (d *Detector) SwitchMode(args *bool, reply *bool) error {
	if *args && !d.config.isGossip { // If switching to gossip, give a grace period to all nodes
		d.lock.Lock()
		for n, m := range d.list {
			m.lastUpdated = time.Now()
			d.list[n] = m
		}
		d.lock.Unlock()
	}
	d.config.isGossip = *args
	return nil
}

func (d *Detector) updateProtocol() {
	if d.config.isGossip {
		d.beatingTicker.Reset(gossipFrequency)
	} else {
		d.beatingTicker.Reset(pingFrequency)
	}

	d.lock.Lock()
	for n := range d.list {
		if n == d.MyNode {
			continue
		}
		server, _ := rpc.Dial("tcp", n.IP()+":8002")
		var reply1 bool
		var reply2 bool
		server.Go("SwitchProtocol.SwitchSuspicion", d.config.suspicionOn, &reply1, nil)
		server.Go("SwitchProtocol.SwitchMode", d.config.isGossip, &reply2, nil)
	}
	for n, m := range d.list { // Always give grace to myself
		m.lastUpdated = time.Now()
		d.list[n] = m
	}
	d.lock.Unlock()
}

func (d *Detector) writeToLogAndStdout(s string) {
	_, _ = d.logFile.WriteString(s)
	_, _ = os.Stdout.WriteString(s)
}

func (d *Detector) GetMembershipList() map[NodeId]localMembership {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return d.list
}

func (d *Detector) ProcessInput(input string) {
	switch input {
	case "list_mem":
		d.lock.RLock()
		for _, m := range d.list {
			_, _ = os.Stdout.WriteString(fmt.Sprintf("%s\n", m))
		}
		d.lock.RUnlock()
		break
	case "list_self":
		_, _ = os.Stdout.WriteString(d.MyNode.String() + "\n")
		break
	case "join": // join executed implicitly when the process starts
		d.introduceMyself()
		d.joinChan <- struct{}{} // resume the go routine that sends heartbeats
		d.joinChan <- struct{}{} // resume the go routine that listens for heartbeats
		d.joinChan <- struct{}{} // resume the go routine that checks for failures
		break
	case "leave":
		d.lock.Lock()
		local := d.list[d.MyNode]
		local.membership.Leaving = true
		d.list[d.MyNode] = local
		d.lock.Unlock()
		d.leaveChan <- struct{}{} // stop the go routine that sends heartbeats

		d.leaveChan <- struct{}{} // stop the go routine that checks for failures

		d.leaveChan <- struct{}{} // stops the goroutine that listens for heartbeats
		for i := 0; i < 3; i++ {  // wait for all 3 go routines to finish leaving
			<-d.doneLeavingChan
		}
		if d.listener != nil {
			_ = d.listener.Close()
		}
		d.lock.Lock()
		clear(d.list)
		d.lock.Unlock()
		break
	case "display_suspects":
		d.lock.RLock()
		for n, m := range d.list {
			if m.membership.Suspicion.Suspected {
				_, _ = os.Stdout.WriteString(fmt.Sprintf("SUSPECTED: %s\n", n))
			}
		}
		d.lock.RUnlock()
		break
	case "switch(gossip, suspect)":
		d.config.suspicionOn = true
		d.config.isGossip = true
		d.updateProtocol()
		break
	case "switch(gossip, nosuspect)":
		d.config.suspicionOn = false
		d.config.isGossip = true
		// Suspicion is turning off, unsuspect everything
		d.lock.Lock()
		for n, m := range d.list {
			m.membership.Suspicion.Suspected = false
			d.list[n] = m
		}
		d.lock.Unlock()
		d.updateProtocol()
		break
	case "switch(ping, suspect)":
		d.config.suspicionOn = true
		d.config.isGossip = false
		d.updateProtocol()
		break
	case "switch(ping, nosuspect)":
		d.config.suspicionOn = false
		d.config.isGossip = false
		// Suspicion is turning off, unsuspect everything
		d.lock.Lock()
		for n, m := range d.list {
			m.membership.Suspicion.Suspected = false
			d.list[n] = m
		}
		d.lock.Unlock()
		d.updateProtocol()
		break
	case "display_protocol":
		if d.config.isGossip {
			fmt.Print("(gossip, ")
		} else {
			fmt.Print("(ping, ")
		}
		if d.config.suspicionOn {
			fmt.Println("suspect)")
		} else {
			fmt.Println("nosuspect)")
		}
		break
	}
}
