package raft

import (
	"context"
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

type CorrelationID string

const CorrelationIDEmpty = CorrelationID("")

func (cID CorrelationID) String() string {
	return string(cID)
}

func NewCorrelationID() CorrelationID {
	var (
		max = correlationIDUpperBoundary
		min = correlationIDLowerBoundary
	)

	return CorrelationID(strconv.Itoa(int(rand.Int63n(int64(max-min))) + min))
}

type contextKey int

const (
	contextKeyCorrelationID contextKey = iota
)

func AddCorrelationID(ctx context.Context, cID CorrelationID) context.Context {
	return context.WithValue(ctx, contextKeyCorrelationID, cID)
}

func GetCorrelationID(ctx context.Context) CorrelationID {
	cID, ok := ctx.Value(contextKeyCorrelationID).(CorrelationID)
	if !ok {
		return CorrelationIDEmpty
	}

	return cID
}
