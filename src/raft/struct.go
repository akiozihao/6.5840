package raft

type Log struct {
	Log    []Entry
	Index0 int
}

func mkLogEmpty() Log {
	return Log{make([]Entry, 1), 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) append(e Entry) {
	l.Log = append(l.Log, e)
}

func (l *Log) start() int {
	return l.Index0
}

func (l *Log) cutEnd(index int) {
	l.Log = l.Log[0 : index-l.Index0]
}

func (l *Log) cutStart(index int) {
	l.Index0 += index
	l.Log = l.Log[index:]
}

func (l *Log) slice(index int) []Entry {
	return l.Log[index-l.Index0:]
}

func (l *Log) lastIndex() int {
	return l.Index0 + len(l.Log) - 1
}

func (l *Log) entry(index int) *Entry {
	return &(l.Log[index-l.Index0])
}

func (l *Log) lastEntry() *Entry {
	return l.entry(l.lastIndex())
}
