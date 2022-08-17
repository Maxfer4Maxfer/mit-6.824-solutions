package raft

import (
	"log"
	"strings"
)

type logTopic string

const (
	appendEntriesLogTopic  logTopic = "APPND"
	applyLogTopic          logTopic = "APPLY"
	commonLogTopic         logTopic = "COMMON"
	heartbeatingLogTopic   logTopic = "HRTBT"
	leaderElectionLogTopic logTopic = "ELECT"
	matchIndexLogTopic     logTopic = "MATCH"
	persisterLogTopic      logTopic = "PRSST"
	startLogTopic          logTopic = "START"
	tickerLogTopic         logTopic = "TICKR"
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
	if cID == "" {
		return l
	}

	return extendLoggerWithPrefix(l, cID, "_")
}
