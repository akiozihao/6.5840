package raft

type Log struct {
	log    []Entry
	index0 int
}

func mkLogEmpty() Log {
	return Log{make([]Entry, 1), 0}
}

func mkLog(log []Entry, index0 int) Log {
	return Log{log, index0}
}

func (l *Log) append(e Entry) {
	l.log = append(l.log, e)
}

func (l *Log) start() int {
	return l.index0
}

func (l *Log) cutend(index int) {
	l.log = l.log[0 : index-l.index0]
}

func (l *Log) cutstart(index int) {
	l.index0 += index
	l.log = l.log[index:]
}

func (l *Log) slice(index int) []Entry {
	return l.log[index-l.index0:]
}

func (l *Log) lastindex() int {
	return l.index0 + len(l.log) - 1
}

func (l *Log) entry(index int) *Entry {
	return &(l.log[index-l.index0])
}

func (l *Log) lastentry() *Entry {
	return l.entry(l.lastindex())
}
