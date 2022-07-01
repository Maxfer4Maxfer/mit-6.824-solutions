package raft

import "log"

type logTopic string

const (
	commonLogTopic         logTopic = "COMMON"
	leaderElectionLogTopic logTopic = "ELECT"
	tickerLogTopic         logTopic = "TICKR"
	heartbeatingLogTopic    logTopic = "HRTBT"
	appendEntriesLogTopic  logTopic = "APPND"
	becomeFollowerLogTopic logTopic = "BCMFL"
)

func extendLoggerWithPrefix(l *log.Logger, pr logTopic) *log.Logger {
	currentPrefix := l.Prefix()
	out := log.New(l.Writer(), currentPrefix, l.Flags())
	out.SetPrefix(currentPrefix + string(pr) + " ")

	return out
}
