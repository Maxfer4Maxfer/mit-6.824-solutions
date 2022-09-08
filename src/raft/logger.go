package raft

import (
	"context"
	"log"
	"strings"
)

type LoggerTopic string

const (
	LoggerTopicAppendEntries   LoggerTopic = "APPND"
	LoggerTopicApply           LoggerTopic = "APPLY"
	LoggerTopicClerk           LoggerTopic = "CLERK"
	LoggerTopicCommon          LoggerTopic = "COMMON"
	LoggerTopicHeartbeating    LoggerTopic = "HRTBT"
	LoggerTopicInstallSnapshot LoggerTopic = "ISNAP"
	LoggerTopicLeaderElection  LoggerTopic = "ELECT"
	LoggerTopicMatchIndex      LoggerTopic = "MATCH"
	LoggerTopicPersister       LoggerTopic = "PRSST"
	LoggerTopicService         LoggerTopic = "SERVC"
	LoggerTopicSnapshot        LoggerTopic = "SNAPS"
	LoggerTopicStart           LoggerTopic = "START"
	LoggerTopicTicker          LoggerTopic = "TICKR"
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
