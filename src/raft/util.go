package raft

import (
	"log"
	"math/rand"
	"strconv"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
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
