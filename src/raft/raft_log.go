package raft

type LogEntry struct {
	Term    int
	Command interface{}
}

func (le *LogEntry) DeepCopy() *LogEntry {
	return &LogEntry{
		Term:    le.Term,
		Command: le.Command,
	}
}

type RLog struct {
	// log entries; each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)
	log []LogEntry

	// first index number in the log
	offset int

	lastIncludedIndex int

	lastIncludedTerm int
}

func newRLog() *RLog {
	return &RLog{
		log:               []LogEntry{},
		offset:            0,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}
}

func (rl *RLog) LastIndex() int {
	return rl.offset + len(rl.log) - 1
}

func (rl *RLog) FirstIndex() int {
	return rl.offset
}

func (rl *RLog) Term(idx int) int {
	if idx-rl.offset == -1 {
		return rl.lastIncludedTerm
	}

	return rl.log[idx-rl.offset].Term
}

func (rl *RLog) Log(idx int) LogEntry {
	return rl.log[idx-rl.offset]
}

func (rl *RLog) Append(es ...LogEntry) {
	rl.log = append(rl.log, es...)
}

func (rl *RLog) Offset() int {
	return rl.offset
}

func (rl *RLog) SetOffset(offset int) {
	rl.offset = offset
}

// LeftShrink delete record from the left.
// The provided idx also be deleted.
func (rl *RLog) LeftShrink(idx int) {
	if idx < rl.LastIndex() {
		rl.log = rl.log[idx-rl.offset+1:]
	} else {
		rl.log = rl.log[:0]
	}

	rl.offset = idx + 1
}

// LeftShrink delete record from the left.
// The provided idx NOT be deleted.
func (rl *RLog) RightShrink(idx int) {
	rl.log = rl.log[:idx-rl.offset]
}

func (rl *RLog) Frame(from, to int) []LogEntry {
	return append([]LogEntry(nil), rl.log[from-rl.offset:to-rl.offset]...)
}
