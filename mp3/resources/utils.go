package resources

import (
	"bytes"
	"cmp"
	"crypto/sha1"
	"encoding/binary"
	"g14-mp4/failureDetector"
	"math/rand"
	"os"
)

// HashNode returns a hash of the node id used to place on the ring
func HashNode(id failureDetector.NodeId) uint32 {
	hasher := sha1.New()
	var buf [12]byte
	binary.BigEndian.PutUint32(buf[0:4], id.Ip)
	binary.BigEndian.PutUint64(buf[4:12], uint64(id.Time))
	hasher.Write(buf[:])
	sum := hasher.Sum(nil)
	return binary.BigEndian.Uint32(sum[0:4]) // range between 0 and 2^32 - 1
}

func HashString(s string) uint32 {
	hasher := sha1.New()
	hasher.Write([]byte(s))
	sum := hasher.Sum(nil)
	return binary.BigEndian.Uint32(sum[0:4])
}

func GetCoordinator() string {
	servers, _ := os.ReadFile("mp3/resources/servers.conf")
	parts := bytes.Split(servers, []byte("\n"))
	n := rand.Intn(len(parts) - 1)
	return string(parts[n])
}

// CompareNodeId compares two NodeId instances first by IP, then by Port, and finally by Time, and returns their order.
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
func CompareNodeId(a, b failureDetector.NodeId) int {
	if c := cmp.Compare(a.Ip, b.Ip); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Port, b.Port); c != 0 {
		return c
	}
	return cmp.Compare(a.Time, b.Time)
}
