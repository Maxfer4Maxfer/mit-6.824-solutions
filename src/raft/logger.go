package raft

import (
	"log"
	"strings"
)

type logTopic string

const (
	commonLogTopic         logTopic = "COMMON"
	leaderElectionLogTopic logTopic = "ELECT"
	tickerLogTopic         logTopic = "TICKR"
	applyLogTopic          logTopic = "APPLY"
	matchIndexLogTopic     logTopic = "MATCH"
	heartbeatingLogTopic   logTopic = "HRTBT"
	startLogTopic          logTopic = "START"
	appendEntriesLogTopic  logTopic = "APPND"
)

func extendLoggerWithPrefix(l *log.Logger, pr string, d string) *log.Logger {
	currentPrefix := l.Prefix()
	currentPrefix = strings.TrimSpace(currentPrefix)

	out := log.New(l.Writer(), currentPrefix, l.Flags())

	out.SetPrefix(currentPrefix + d + pr + " ")

	return out
}

func extendLoggerWithTopic(l *log.Logger, pr logTopic) *log.Logger {
	return extendLoggerWithPrefix(l, string(pr), " ")
}

func extendLoggerWithCorrelationID(l *log.Logger, cID string) *log.Logger {
	return extendLoggerWithPrefix(l, cID, "_")
}
