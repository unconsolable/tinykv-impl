// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	// Index and term before the first entry
	firstLogIdx  uint64
	firstLogTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	ret := &RaftLog{storage: storage, entries: make([]pb.Entry, 0)}
	// Get the first and last index of previous entries
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err.Error())
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err.Error())
	}
	ret.firstLogIdx = firstIndex - 1
	if firstLogTerm, err := storage.Term(ret.firstLogIdx); err != nil {
		panic(err.Error())
	} else {
		ret.firstLogTerm = firstLogTerm
	}
	// Fetch previous entries and append
	prevEntries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err.Error())
	}
	ret.entries = append(ret.entries, prevEntries...)
	// Get stabled index
	if stabled, err := storage.LastIndex(); err != nil {
		panic(err.Error())
	} else {
		ret.stabled = stabled
	}
	// First log idx and term will be zero before snapshot is introduced
	return ret
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	offset := l.entries[0].Index
	return l.entries[l.stabled-offset+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	offset := l.entries[0].Index
	return l.entries[l.applied-offset+1 : l.committed-offset+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.firstLogIdx
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
// or firstLogTerm if i == firstLogIdx
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == l.firstLogIdx {
		return l.firstLogTerm, nil
	} else if len(l.entries) == 0 {
		return 0, ErrUnavailable
	}
	offset := l.entries[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(l.entries) {
		return 0, ErrUnavailable
	}
	return l.entries[i-offset].Term, nil
}

// Entries returns a slice of log entries in the range [lo, hi)
func (l *RaftLog) Entries(lo, hi uint64) ([]*pb.Entry, error) {
	offset := l.entries[0].Index
	if lo < offset {
		return nil, ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, l.LastIndex())
	}
	ents := l.entries[lo-offset : hi-offset]
	ret := make([]*pb.Entry, len(ents))
	for i := range ents {
		ret[i] = &ents[i]
	}
	return ret, nil
}

// Log entry append for leader
func (l *RaftLog) LeaderAppend(data []byte, term uint64) error {
	lastIndex := l.LastIndex()
	l.entries = append(l.entries, pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      term,
		Index:     lastIndex + 1,
		Data:      data,
	})
	return nil
}

func (l *RaftLog) NonLeaderCheck(m pb.Message) (bool, error, *pb.AppendRejectHint) {
	// m.Entries before firstLogIdx, cut to the same
	if m.Index < l.firstLogIdx {
		m.Index = l.firstLogIdx
		m.LogTerm = l.firstLogTerm
		i := 0
		for ; i < len(m.Entries); i++ {
			if m.Entries[i].Index == m.Index {
				if m.Entries[i].Term != m.LogTerm {
					panic("m.Entries[i].Term != m.Term")
				}
				i++
				break
			}
		}
		m.Entries = m.Entries[i:]
	}
	// Reject and REPLY false if log does not contain an entry
	// at prevLogIndex whose Term matches prevLogTerm
	prevIdxTerm, err := l.Term(m.Index)
	if err != nil && err != ErrUnavailable {
		panic(err.Error())
	}
	if m.Index > l.LastIndex() {
		hint := &pb.AppendRejectHint{
			XLen:     l.LastIndex() + 1,
			XTerm:    0,
			XIndex:   0,
			HasXTerm: false,
		}
		return true, nil, hint
	}
	if prevIdxTerm != m.LogTerm {
		index, last := l.firstLogIdx, l.LastIndex()
		for ; index <= last; index++ {
			if term, err := l.Term(index); err != nil {
				panic(err.Error())
			} else if term == prevIdxTerm {
				break
			}
		}
		hint := &pb.AppendRejectHint{
			XLen:     l.LastIndex() + 1,
			XTerm:    prevIdxTerm,
			XIndex:   index,
			HasXTerm: true,
		}
		return true, nil, hint
	}
	return false, nil, nil
}

// Log entry append for non-leader, return true if rejected
func (l *RaftLog) NonLeaderAppend(m pb.Message) {
	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it
	i, first, last, lenMsgEnts := uint64(0), m.Index+1, l.LastIndex(), uint64(len(m.Entries))
	if len(l.entries) > 0 {
		offset := l.entries[0].Index
		for ; i < lenMsgEnts && i+first <= last; i++ {
			logTerm, err := l.Term(i + first)
			if err != nil {
				panic(err.Error())
			}
			if m.Entries[i].Index != i+first {
				panic("m.Entries[i].Index != i+first")
			}
			if m.Entries[i].Term != logTerm {
				l.entries = l.entries[:i+first-offset]
				break
			}
		}
	}
	// If a follower receives an AppendEntries request that includes log entries
	// already present in its log, it ignores those entries in the new request.
	m.Entries = m.Entries[i:]
	// Update stabled index, then append
	if storLastIdx, err := l.storage.LastIndex(); err != nil {
		panic(err.Error())
	} else {
		l.stabled = min(l.LastIndex(), storLastIdx)
	}
	for _, ent := range m.Entries {
		l.entries = append(l.entries, *ent)
	}
}
