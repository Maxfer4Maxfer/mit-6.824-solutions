package raft

import (
	crand "crypto/rand"
	"log"
	"math/big"
	"math/rand"
	"strconv"
)

func MakeSeed() int64 {
	shift := 62

	max := big.NewInt(int64(1) << shift)
	bigx, _ := crand.Int(crand.Reader, max)

	x := bigx.Int64()

	return x
}

// Debugging.
const Debug = false

func DPrintf(format string, a ...interface{}) (int, error) {
	if Debug {
		log.Printf(format, a...)
	}

	return 0, nil
}

const (
	correlationIDUpperBoundary = 99999
	correlationIDLowerBoundary = 10000
)

func CorrelationID() string {
	var (
		max = correlationIDUpperBoundary
		min = correlationIDLowerBoundary
	)

	return strconv.Itoa(int(rand.Int63n(int64(max-min))) + min)
}
