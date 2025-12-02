package resources

import (
	"crypto/sha1"
	"encoding/binary"
)

func HashString(s string) int {
	hasher := sha1.New()
	hasher.Write([]byte(s))
	sum := hasher.Sum(nil)
	return int(binary.BigEndian.Uint32(sum[0:4]))
}
