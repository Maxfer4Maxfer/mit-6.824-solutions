package raft

import (
	"context"
	"log"
	"strings"
)

type LoggerTopic string

const (
	LoggerTopicEmpty        LoggerTopic = ""
	appendEntriesLogTopic   LoggerTopic = "APPND"
	applyLogTopic           LoggerTopic = "APPLY"
	commonLogTopic          LoggerTopic = "COMMON"
	heartbeatingLogTopic    LoggerTopic = "HRTBT"
	installSnapshotLogTopic LoggerTopic = "ISNAP"
	leaderElectionLogTopic  LoggerTopic = "ELECT"
	matchIndexLogTopic      LoggerTopic = "MATCH"
	persisterLogTopic       LoggerTopic = "PRSST"
	snapshotLogTopic        LoggerTopic = "SNAPS"
	startLogTopic           LoggerTopic = "START"
	tickerLogTopic          LoggerTopic = "TICKR"
)

func extendLoggerWithPrefix(l *log.Logger, pr string, d string) *log.Logger {
	currentPrefix := l.Prefix()
	currentPrefix = strings.TrimSpace(currentPrefix)

	out := log.New(l.Writer(), currentPrefix, l.Flags())

	out.SetPrefix(currentPrefix + d + pr + " ")

	return out
}

func extendLoggerWithTopic(l *log.Logger, lt LoggerTopic) *log.Logger {
	return extendLoggerWithPrefix(l, string(lt), " ")
}

func extendLoggerWithCorrelationID(l *log.Logger, cID CorrelationID) *log.Logger {
	if cID == "" {
		return l
	}

	return extendLoggerWithPrefix(l, cID.String(), "_")
}

func extendLogger(
	ctx context.Context, l *log.Logger, lt LoggerTopic,
) *log.Logger {
	nl := extendLoggerWithPrefix(l, string(lt), " ")
	nl = extendLoggerWithCorrelationID(nl, getCorrelationID(ctx))

	return nl
}
