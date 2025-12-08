# RainStorm

RainStorm is a distributed stream processing system implemented in Go. Inspired by frameworks like Apache Storm, it enables real-time data processing through a pipeline of stages (tasks) distributed across a cluster of worker nodes. It supports fault tolerance, dynamic autoscaling, and integration with a distributed file system (HyDFS).

## Architecture

RainStorm consists of two main components:

1.  **RainStorm Leader:** The central coordinator responsible for:
    *   Scheduling tasks to workers.
    *   Monitoring system health and task rates.
    *   Handling fault tolerance (reassigning failed tasks).
    *   Managing autoscaling (upscaling/downscaling tasks based on load).
    *   Acting as the entry point for data injection.
2.  **RainStorm Worker:** The node responsible for executing the actual processing logic.
    *   Runs local executables for specific tasks (Filter, Transform, Aggregate).
    *   Handles tuple routing between stages using TCP.
    *   Reports status and metrics back to the Leader.
    *   Logs processed data to HyDFS for recovery and persistence.

## Features

*   **Distributed Processing:** Tasks are split into stages and distributed across available worker nodes.
*   **Fault Tolerance:** The Leader detects worker failures and automatically reschedules tasks to healthy nodes. Workers use write-ahead logging to recover state.
*   **Autoscaling:** The system monitors input rates at each stage and automatically adds or removes tasks to maintain throughput within defined watermarks.
*   **Exactly-Once Processing:** (Configurable) Mechanisms to ensure tuples are processed reliably even in the event of failures.
*   **HyDFS Integration:** Reads source data from and writes final output to a custom distributed file system (HyDFS).

## Project Structure

```
RainStorm/
├── RainStormLeader/    # Source code for the Leader node
│   └── main.go
├── RainStormWorker/    # Source code for the Worker nodes
│   └── main.go
├── resources/          # Shared types and utility functions
│   ├── types.go
│   └── utils.go
└── tasks/              # User-defined stream processing operations
    ├── AggregateByKey/ # CSV aggregation logic
    ├── Filter/         # Simple grep-like filtering
    └── Transform/      # CSV column transformation
```


## Getting Started

### Prerequisites

*   **Go 1.24+**
*   **HyDFS:** A running instance of the HyDFS distributed file system (listening on port `8011` locally).

### Compilation

Build the leader, worker, and task binaries:

```shell script
# Build Leader
go build -o leader RainStorm/RainStormLeader/main.go

# Build Worker
go build -o worker RainStorm/RainStormWorker/main.go

# Build Tasks (These must be available in the path or specified location)
go build -o Filter RainStorm/tasks/Filter/main.go
go build -o Transform RainStorm/tasks/Transform/main.go
go build -o AggregateByKey RainStorm/tasks/AggregateByKey/main.go
```


### Running the System

1.  **Start the Leader:**
    Run the leader on a designated machine. It listens for CLI commands on Stdin.

```shell script
./leader
```


2.  **Start Workers:**
    Run the worker binary on all compute nodes. They will register themselves with the leader via RPC.

```shell script
./worker
```


3.  **Submit a Job:**
    Interact with the Leader via Stdin to submit a RainStorm job. The command format is:

```plain text
RainStorm <Op1> <Args1> <Op2> <Args2> ... <SrcFile> <DestFile> <NumTasks>
```

    *(Note: The actual parsing logic allows for specific flags like `ExactlyOnce`, `AutoScale`, etc. See usage example below).*

### CLI Usage Example

Inside the running Leader process, you can submit a job using the following format:

```plain text
RainStorm [Stages] [TasksPerStage] [Op1] [Arg1] [Op2] [Arg2] ... [SrcHydfsFile] [DestHydfsFile] [ExactlyOnce] [AutoScale] [InputRate] [LowWatermark] [HighWatermark]
```


**Example:**
```plain text
RainStorm 2 3 Filter "error" Count "0" logs.csv output.txt true true 1000 0.2 0.8
```


This command starts a job with:
*   **2 Stages**, **3 Tasks** per stage initially.
*   **Stage 1:** `Filter` operation searching for "error".
*   **Stage 2:** `Count` (Aggregate) operation on column 0.
*   **Source:** `logs.csv` (from HyDFS).
*   **Destination:** `output.txt`.
*   **Configs:** ExactlyOnce=true, AutoScale=true, InputRate=1000 tuples/sec.

### Administrative Commands

The Leader supports additional commands during runtime:

*   `list_tasks`: Displays the current distribution of tasks, including IP, PID, and Operation.
*   `kill_task <VM_IP> <PID>`: Simulates a failure by killing a specific task process. The Leader should detect this and reschedule the task.

## Communication Ports

*   `8020`: Introduce/Registration Port
*   `8021`: Assignment Port (Leader -> Worker commands)
*   `8022`: Tuple Port (Data transfer between tasks)
*   `8023`: Global Resource Manager Port
*   `8024`: Ack Port