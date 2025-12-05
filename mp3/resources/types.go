package resources

import (
	"container/list"
	"encoding/gob"
	. "g14-mp4/failureDetector"
)

func init() {
	// Register any custom errors/types
	gob.Register(&FileAlreadyExistsError{})
	gob.Register(&FileNotFoundError{})
}

// File holds the file Name and contents not yet written to disk in the AppendBuffer
type File struct {
	Name         string
	AppendBuffer *list.List
}

type ExportedFile struct {
	Name         string
	AppendBuffer []AppendBlock
}

func (f *File) Init(name string) {
	f.Name = name
	f.AppendBuffer = list.New()
}

// AppendNumber is a tuple that contains the NodeId of the client and Number of appends sent so far
type AppendNumber struct {
	NodeId  NodeId
	Counter int
}

// AppendBlock is the type of each node in File.AppendBuffer
type AppendBlock struct {
	AppendNumber AppendNumber
	Data         []byte
}

type CoordinatorArgs struct {
	HDFSFileName string
}

type CoordinatorReply struct {
	Nodes []NodeId
}

type AddFileArgs struct {
	HDFSFileName string
	Content      []byte
	AppendNumber AppendNumber
}

type AddFileReply struct {
	Err error
}

type GetFileArgs struct {
	HDFSFileName string
}

type AppendArgs = AddFileArgs

type AppendReply = AddFileReply
type FileAlreadyExistsError struct {
	FileName string
}

func (f *FileAlreadyExistsError) Error() string {
	return "file already exists: " + f.FileName
}

type FileNotFoundError struct {
	FileName string
}

func (f *FileNotFoundError) Error() string {
	return "file not found: " + f.FileName
}

type MultiAppendArgs struct {
	LocalName  string
	RemoteName string
}

type SendFileArgs struct {
	FileName      string
	DiskContent   []byte
	BufferContent []AppendBlock
}

// RemoteFileArgs is used to send an append or create call to the server
type RemoteFileArgs struct {
	RemoteName string
	Content    []byte
}
