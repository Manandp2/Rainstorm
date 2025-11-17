# **HyDFS**

## **System Architecture**

### **1\. Replication**

Files are hashed to a point on the logical ring using SHA-1.
The file is then stored on the next three nodes (Primary, Successor 1, and Successor 2\) clockwise from that point.
The client's RequestCoordinator RPC call determines these three nodes.

### **2\. Append & Merge Process**

We use an append first model:

1. **Append:** A client appends data by contacting two of the three replicas.
2. **Buffering:** The server receives the append and inserts the data into an in-memory linked list (AppendBuffer), sorted by a unique AppendNumber (Client NodeId \+ Counter). This avoids immediate disk I/O and expensive locking.
3. **Merge on Read/Get:** When a client issues a get, the receiving node (Replica A) fetches the AppendBuffer from another node (Replica B). It merges the two sorted lists, appends the result to its on-disk file content, and returns the complete file to the client.
4. **Merge Command:** A manual merge command forces a full merge across all three replicas, writes the result to disk, and clears the in-memory buffers. This also runs in the background every minute.

### **3\. Failure Handling**

* **Node Join:** When a node joins, files are redistributed. Nodes that are no longer responsible for a replica send their copy to the new node (SendFile RPC) and delete it locally.
* **Node Failure:** When a node fails, the remaining replicas for an affected file will re-replicate the data to a new node to maintain the 3x replication factor.

## **Available Commands**

| Command              | Arguments                    | Description                                                |
|:---------------------|:-----------------------------|:-----------------------------------------------------------|
| **create**           | \[localName\] \[remoteName\] | Uploads a new file to the cluster.                         |
| **append**           | \[localName\] \[remoteName\] | Appends the contents of a local file to a remote file.     |
| **get**              | \[remoteName\] \[localName\] | Downloads a file from the cluster.                         |
| **merge**            | \[remoteName\]               | Forces a cluster-wide merge of append buffers for a file.  |
| **ls**               | \[remoteName\]               | Lists the file's hash and its 3 replica nodes.             |
| **list\_mem\_ids**   |                              | Lists all NodeIds in the current membership list.          |
| **liststore**        |                              | (Server-side) Lists all files stored locally on this node. |
| **join** / **leave** |                              | (Server-side) Commands to join or leave the cluster.       |
