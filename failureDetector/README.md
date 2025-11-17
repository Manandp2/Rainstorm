# MP2 Failure Detector

This project provides a distributed `grep` utility that allows you to run `grep` commands across multiple servers simultaneously from a single client. It consists of a `grep-daemon` that runs on each server and a `dist-grep` client that sends commands and aggregates the results.

***

## Requirements


* **Go**: Version `1.24` or higher must be installed on the client machine and all server machines.

***

## Setup and Usage

Follow these steps to set up and run the failure detector.

### 1. Clone the Repository

On **all servers**, clone this repository to your desired directory.

```bash
  git clone https://gitlab.engr.illinois.edu/manandp2/g14-mp2
  cd g14-mp2
```

### 2. Build the Project
If you want a different introducer, change all instances of `fa25-cs425-1401.cs.illinois.edu` to the hostname of your desired introducer.

On **all servers**, build the binaries from the project's root directory. This command will compile the source code and place the executables (`g14-mp2`) in your `$GOPATH/bin` directory (usually `~/go/bin/`).

```bash
  go install ./...
```

### 3. Run the Failure Detector

**First**, run `g14-mp2` on the introducer.

Now, on **all other servers**, start `g14-mp2`. 
```bash
  ~/go/bin/g14-mp2
```

### 5. Failure Detection

You are now ready to detect failures. 
On any machine, kill the process, and it will be detected by the other machines.

### 6. Commands

| Command                   | Description                                                                              |
|---------------------------|------------------------------------------------------------------------------------------|
| list_mem                  | Prints the current node's complete membership list with the status of each member.       |
| list_self                 | Prints the unique NodeId of the current node.                                            |
| leave                     | Gracefully removes the current node from the group and terminates its network listeners. |
| join                      | Allows a node that has leaved to rejoin the group. The node will get a new NodeId.       |
| display_protocol          | Shows the currently active protocol, e.g., (gossip, suspect).                            |
| display_suspects          | Lists all nodes currently marked as suspected.                                           |
| switch(gossip, suspect)   | Switches the entire network to Gossip with the suspicion mechanism enabled.              |
| switch(gossip, nosuspect) | Switches the entire network to Gossip without suspicion.                                 |
| switch(ping, suspect)     | Switches the entire network to Ping-Ack with the suspicion mechanism enabled.            |
| switch(ping, nosuspect)   | Switches the entire network to Ping-Ack without suspicion.                               |