# MP1 Distributed Grep

This project provides a distributed `grep` utility that allows you to run `grep` commands across multiple servers simultaneously from a single client. It consists of a `grep-daemon` that runs on each server and a `dist-grep` client that sends commands and aggregates the results.

***

## Requirements 


* **Go**: Version `1.24` or higher must be installed on the client machine and all server machines.
* **Logfiles**: The log files you intend to search must be present on the respective servers.

***

## Setup and Usage 

Follow these steps to set up and run the distributed grep utility.

### 1. Clone the Repository

On **all servers** and the **client machine**, clone this repository to your desired directory.

```bash
git clone <your-repository-url>
cd <repository-directory>
```

### 2. Build the Project

On **all servers** and the **client machine**, build the binaries from the project's root directory. This command will compile the source code and place the executables (`grep-daemon` and `dist-grep`) in your `$GOPATH/bin` directory (usually `~/go/bin/`).

```bash
go install ./...
```

### 3. Run the Daemon on Servers

On **each server**, start the `grep-daemon` as a background process. This ensures the daemon continues running even if you close your terminal session.

```bash
nohup ~/go/bin/grep-daemon & disown
```

### 4. Configure the Client

On the **client machine**, you need to tell the `dist-grep` tool which servers to contact. Update the `servers.conf` file with the network addresses (IP address and Port) of all the servers running the `grep-daemon`. The default Port for the daemon is `8000`.

**Example `servers.conf`:**
```
fa25-cs425-1401.cs.illinois.edu:8000
fa25-cs425-1402.cs.illinois.edu:8000
fa25-cs425-1403.cs.illinois.edu:8000
fa25-cs425-1404.cs.illinois.edu:8000
fa25-cs425-1405.cs.illinois.edu:8000
fa25-cs425-1406.cs.illinois.edu:8000
fa25-cs425-1407.cs.illinois.edu:8000
fa25-cs425-1408.cs.illinois.edu:8000
fa25-cs425-1409.cs.illinois.edu:8000
fa25-cs425-1410.cs.illinois.edu:8000
```

### 5. Run a Distributed Grep

You are now ready to perform a distributed search. On the **client machine**, run the `dist-grep` command followed by any standard `grep` arguments. The command will be executed on all configured servers, and the combined results will be streamed to your terminal.

**Example:**

```bash
~/go/bin/dist-grep -i "args" 
```