package raft

import (
	"context"
	"log"
	"strings"
)

type LoggerTopic string

const (
	LoggerTopicEmpty        LoggerTopic = ""
	LoggerTopicClerk        LoggerTopic = "CLERK"
	LoggerTopicService      LoggerTopic = "SERVC"
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

func ExtendLoggerWithTopic(l *log.Logger, lt LoggerTopic) *log.Logger {
	return extendLoggerWithPrefix(l, string(lt), " ")
}

func ExtendLoggerWithCorrelationID(l *log.Logger, cID CorrelationID) *log.Logger {
	if cID == "" {
		return l
	}

	return extendLoggerWithPrefix(l, cID.String(), "_")
}

func ExtendLogger(
	ctx context.Context, l *log.Logger, lt LoggerTopic,
) *log.Logger {
	nl := extendLoggerWithPrefix(l, string(lt), " ")
	nl = ExtendLoggerWithCorrelationID(nl, GetCorrelationID(ctx))

	return nl
}
