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
	"errors"
	"log"
	"math/rand"
	"sort"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// half election timeout, used for reset electionElapsed
	halfElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// Set random seed
	rand.Seed(time.Now().UnixNano())
	// Recover HardState
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	if len(confState.Nodes) == 0 {
		confState.Nodes = c.peers
	}
	// Init Raft state, set to follower
	ret := &Raft{
		id:                  c.ID,
		electionTimeout:     2 * c.ElectionTick,
		heartbeatTimeout:    c.HeartbeatTick,
		Term:                hardState.Term,
		Vote:                hardState.Vote,
		State:               StateFollower,
		Prs:                 make(map[uint64]*Progress),
		halfElectionTimeout: c.ElectionTick,
		electionElapsed:     rand.Intn(c.ElectionTick) + 1,
	}
	for _, v := range confState.Nodes {
		ret.Prs[v] = &Progress{}
	}
	// Recover RaftLog
	ret.RaftLog = newLog(c.Storage)
	ret.RaftLog.committed = hardState.Commit
	if c.Applied != 0 {
		ret.RaftLog.applied = c.Applied
	} else {
		ret.RaftLog.applied = ret.RaftLog.firstLogIdx
	}
	ret.updatePengingConfIdx()
	DPrintf("New Raft=%+v\n", *ret.RaftLog)
	return ret
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// Calculate prevLogIndex, prevLogTerm
	reqMsg := pb.Message{}
	if r.Prs[to].Next <= r.RaftLog.firstLogIdx {
		// PrevLog has been compacted, send Snapshot RPC
		snap, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			// Snapshot is not ready, send heartbeat instead
			r.sendHeartbeat(to)
			return true
		}
		reqMsg = pb.Message{
			MsgType:  pb.MessageType_MsgSnapshot,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			Snapshot: &snap,
		}
	} else {
		prevLogIndex := r.Prs[to].Next - 1
		prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
		if err != nil {
			panic(err.Error())
		}
		// Fetch corresponding entries[]
		entries, err := r.RaftLog.Entries(r.Prs[to].Next, r.RaftLog.LastIndex()+1)
		if err != nil {
			panic(err.Error())
		}
		reqMsg = pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: prevLogTerm,
			Index:   prevLogIndex,
			Entries: entries,
			Commit:  r.RaftLog.committed,
		}
	}
	r.msgs = append(r.msgs, reqMsg)
	DPrintf("%v Send AppendEntries, Msg %+v", r.id, reqMsg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	reqMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, reqMsg)
	DPrintf("%d send heartbeat, msg: %+v\n", r.id, reqMsg)
}

// sendRequestVote sends a RequestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// Your Code Here (2A).
	lastTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		panic(err.Error())
	}
	reqMsg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastTerm,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, reqMsg)
	DPrintf("%d send RequestVote to %d\n", r.id, to)
}

func (r *Raft) sendTimeout(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
	})
}

func (r *Raft) sendTransfer(to uint64) {
	if to == None {
		log.Printf("Don't know leader")
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTransferLeader,
		To:      to,
		From:    r.id,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.leadTransferee != None {
			r.electionElapsed++
			if r.electionElapsed >= r.electionTimeout {
				// transfer fail, resume operation
				r.leadTransferee = None
				r.electionElapsed = 0
			}
		}
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.bcastHeartbeat()
		}
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			r.campaign()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// Reset term, leader, election timeout, vote record, leaderTransferee
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	r.leadTransferee = None
	// Switch state to follower
	r.State = StateFollower
	DPrintf("%d become Follower, term %v\n", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// Update term, vote to itself, reset election timeout, leaderTransferee
	r.Term++
	r.Vote = r.id
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	r.leadTransferee = None
	// Reset votes
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	// Switch state to candidate
	r.State = StateCandidate
	DPrintf("%d become Candidate, term %v\n", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// Reset heartbeat tick
	r.heartbeatElapsed = 0
	// Init progress
	r.updatePengingConfIdx()
	for pr := range r.Prs {
		r.Prs[pr] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
	r.Prs[r.id].Match = r.Prs[r.id].Next - 1
	// Propose a noop entry on its term
	noOp := pb.Entry{}
	r.appendEntry(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{&noOp},
	})
	// Switch state to leader
	r.Lead = r.id
	r.State = StateLeader
	DPrintf("%d become Leader, term %v\n", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// Check the terms of node and incoming message to prevent stale log entries
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.campaign()
	case pb.MessageType_MsgTimeoutNow:
		// If it has been removed from group, nothing happens
		if _, ok := r.Prs[r.id]; ok {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.sendTransfer(r.Lead)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgPropose:
	default:
		log.Fatalf("Implement message %v in %v", m.MsgType, r.State.String())
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.campaign()
	case pb.MessageType_MsgTimeoutNow:
		// If it has been removed from group, nothing happens
		if _, ok := r.Prs[r.id]; ok {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		r.sendTransfer(r.Lead)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppendResponse:
	default:
		log.Fatalf("Implement message %v in %v", m.MsgType, r.State.String())
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgPropose:
		r.appendEntry(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResp(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResp(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleLeaderTransfer(m)
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgTimeoutNow:
	default:
		log.Fatalf("Implement message %v in %v", m.MsgType, r.State.String())
	}
	return nil
}

// campaign starts a new election
func (r *Raft) campaign() {
	// Count current votes
	cnt := 0
	for _, v := range r.votes {
		if v {
			cnt++
		}
	}
	// Ensure len(r.Prs) > 0 to avoid the replicated peer
	// start the election, vote for itself, become leader, then panic
	if cnt*2 > len(r.Prs) && len(r.Prs) > 0 {
		// Win majority
		r.becomeLeader()
		return
	}
	// Not enough, start campaign
	for pr := range r.Prs {
		if pr == r.id {
			continue
		}
		r.sendRequestVote(pr)
	}
}

// appendEntry handle MsgPropose message
func (r *Raft) appendEntry(m pb.Message) {
	// Your Code Here (2A).
	if r.leadTransferee != None {
		// Leader transfer, stop accepting new proposal
		return
	}
	// append entries to log, and update matchIndex
	for _, entry := range m.Entries {
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > r.RaftLog.applied {
				// Refuse to propose
				entry.EntryType, entry.Data = pb.EntryType_EntryNormal, nil
			} else {
				r.PendingConfIndex = r.RaftLog.LastIndex() + 1
			}
		}
		r.RaftLog.LeaderAppend(entry, r.Term)
		r.Prs[r.id].Match++
		r.Prs[r.id].Next++
	}
	// With less than 2 nodes, leader itself can be majority
	if len(r.Prs) < 2 {
		r.updateCommitted()
		return
	}
	r.bcastAppend()
}

// bcastAppend send entries to peers
func (r *Raft) bcastAppend() {
	// Your Code Here (2A).
	for pr := range r.Prs {
		if pr == r.id {
			continue
		}
		// send AppendEntries RPC with log entries starting at nextIndex
		r.sendAppend(pr)
	}
}

// bcastHeartbeat send heartbeat to peers
func (r *Raft) bcastHeartbeat() {
	// Your Code Here (2A).
	for pr := range r.Prs {
		if pr == r.id {
			continue
		}
		r.sendHeartbeat(pr)
	}
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A).
	respMsg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  true,
	}
	defer func(msg *pb.Message) {
		r.msgs = append(r.msgs, *msg)
	}(&respMsg)
	// Reply false if term < currentTerm
	if m.Term < r.Term {
		return
	}
	// Update term if find higher
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		respMsg.Term = r.Term
	}
	// candidate’s log is at least as up-to-date as receiver’s log
	lastLogTerm, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		panic(err.Error())
	}
	if !((m.LogTerm > lastLogTerm) || (m.LogTerm == lastLogTerm && m.Index >= r.RaftLog.LastIndex())) {
		return
	}
	// Raft.Vote is null, only vote once
	if r.Vote == None || r.Vote == m.From {
		DPrintf("%d grant vote to %d\n", r.id, m.From)
		r.Vote = m.From
		respMsg.Reject = false
		r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	}
}

// handleRequestVote handle RequestVote RPC response
func (r *Raft) handleRequestVoteResp(m pb.Message) {
	// Your Code Here (2A).
	// Record vote results
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.votes[m.From] = false
		}
	} else {
		r.votes[m.From] = true
	}
	// Count vote
	aye, nay := 0, 0
	for _, v := range r.votes {
		if v {
			aye++
		} else {
			nay++
		}
	}
	// Ensure len(r.Prs) > 0 to avoid the replicated peer
	// start the election, vote for itself, become leader, then panic
	if len(r.Prs) == 0 || nay*2 > len(r.Prs) {
		// Lose majority
		r.becomeFollower(r.Term, None)
	}
	if aye*2 > len(r.Prs) {
		// Win majority
		r.becomeLeader()
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	respMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Reject:  false,
		Index:   m.Index + uint64(len(m.Entries)),
	}
	defer func(msg *pb.Message) {
		r.msgs = append(r.msgs, *msg)
	}(&respMsg)
	// Reply false if Term < Raft.Term
	if m.Term < r.Term {
		respMsg.Reject = true
		return
	}
	// If RPC request or response contains term T > currentTerm,
	// or as large as the candidate’s current term
	// set currentTerm = T, convert to follower
	if m.Term > r.Term || (m.Term == r.Term && r.State == StateCandidate) {
		r.becomeFollower(m.Term, m.From)
	}
	respMsg.Term = r.Term
	// Record leader id
	r.Lead = m.From
	// Reset election timer
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	// Check log entries
	if rejected, err, hint := r.RaftLog.NonLeaderCheck(m); err != nil {
		panic(err.Error())
	} else {
		respMsg.Reject = rejected
		if rejected {
			respMsg.Hint = hint
			return
		}
	}
	// Append log entries
	r.RaftLog.NonLeaderAppend(m)
	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last NEW entry)
	newCommitted := min(m.Commit, respMsg.Index)
	if newCommitted > r.RaftLog.committed {
		r.RaftLog.committed = newCommitted
	}
}

// handleAppendEntries handle AppendEntries RPC response
func (r *Raft) handleAppendEntriesResp(m pb.Message) {
	// Your Code Here (2A).
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		} else {
			if m.Hint.HasXTerm {
				// leader doesn't have XTerm: nextIndex = XIndex
				// leader has XTerm: nextIndex = leader's last entry for XTerm
				next := r.Prs[m.From].Next - 1
				for ; next > r.RaftLog.firstLogIdx; next-- {
					if term, err := r.RaftLog.Term(next); err != nil {
						panic(err.Error())
					} else if term <= m.Hint.XTerm {
						break
					}
				}
				if term, _ := r.RaftLog.Term(next); next != r.RaftLog.firstLogIdx && term == m.Hint.XTerm {
					r.Prs[m.From].Next = next
				} else {
					r.Prs[m.From].Next = m.Hint.XIndex
				}
			} else {
				// follower's log is too short: Next = XLen
				r.Prs[m.From].Next = m.Hint.XLen
			}
			r.sendAppend(m.From)
			return
		}
	}
	// m.Index store the last index of reqMsg.entries
	if m.Index != 0 {
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From].Next = m.Index + 1
			r.Prs[m.From].Match = m.Index
		}
		if m.From == r.leadTransferee && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
			// Catch up successful
			r.sendTimeout(m.From)
		}
		r.updateCommitted()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// respMsg.Index to indicate whether it has up-to-date log
	respMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  false,
	}
	defer func(msg *pb.Message) {
		r.msgs = append(r.msgs, *msg)
	}(&respMsg)
	// Reply false if Term < Raft.Term
	if m.Term < r.Term {
		respMsg.Reject = true
		return
	}
	// If RPC request or response contains term T > currentTerm,
	// or as large as the candidate’s current term
	// set currentTerm = T, convert to follower
	if m.Term > r.Term || (m.Term == r.Term && r.State == StateCandidate) {
		r.becomeFollower(m.Term, m.From)
	}
	respMsg.Term = r.Term
	// Record leader id
	r.Lead = m.From
	// Reset election timer
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
}

// handleHeartbeatResp handle Heartbeat RPC response
func (r *Raft) handleHeartbeatResp(m pb.Message) {
	DPrintf("%d receive Heartbeat Resp, msg: %+v\n", r.id, m)
	if m.Index < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// Update committed for leader
func (r *Raft) updateCommitted() {
	if len(r.Prs) == 0 {
		return
	}
	matchIdxs, i := make([]uint64, len(r.Prs)), 0
	for _, v := range r.Prs {
		matchIdxs[i] = v.Match
		i++
	}
	sort.Sort(uint64Slice(matchIdxs))
	newCommitted := matchIdxs[len(r.Prs)/2]
	if len(r.Prs)%2 == 0 {
		newCommitted = matchIdxs[len(r.Prs)/2-1]
	}
	// If there exists an N such that N > commitIndex, ...
	if newCommitted <= r.RaftLog.committed {
		return
	}
	// ...a majority of matchIndex[i] >= N, and log[N].term == currentTerm:
	// set commitIndex = N, then return true
	if newCommitted <= r.RaftLog.firstLogIdx {
		r.RaftLog.committed = newCommitted
		r.bcastAppend()
	} else if term, err := r.RaftLog.Term(newCommitted); err != nil {
		panic(err.Error())
	} else if term == r.Term {
		r.RaftLog.committed = newCommitted
		r.bcastAppend()
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	respMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Reject:  false,
	}
	defer func(msg *pb.Message) {
		r.msgs = append(r.msgs, *msg)
	}(&respMsg)
	// Reply immediately if term < currentTerm
	if m.Term < r.Term {
		respMsg.Reject = true
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	respMsg.Term = r.Term
	// Record leader id
	r.Lead = m.From
	// Reset election timer
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	// Save snapshot file, discard any existing or partial snapshot
	// with a smaller index
	if r.RaftLog.pendingSnapshot == nil || m.Snapshot.Metadata.Index > r.RaftLog.pendingSnapshot.Metadata.Index {
		r.RaftLog.pendingSnapshot = m.Snapshot
	}
	respMsg.Index = r.RaftLog.pendingSnapshot.Metadata.Index
	// Reset state machine using snapshot contents
	r.RaftLog.firstLogIdx = r.RaftLog.pendingSnapshot.Metadata.Index
	r.RaftLog.firstLogTerm = r.RaftLog.pendingSnapshot.Metadata.Term
	if r.RaftLog.committed < r.RaftLog.pendingSnapshot.Metadata.Index {
		r.RaftLog.committed = r.RaftLog.pendingSnapshot.Metadata.Index
	}
	r.RaftLog.handleEntsAfterSnap()
	r.Prs = make(map[uint64]*Progress)
	for _, v := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[v] = &Progress{}
	}
}

// handle LeaderTransfer
func (r *Raft) handleLeaderTransfer(m pb.Message) {
	if r.id == m.From {
		// Transfer leadership back to self when last transfer is pending.
		if r.leadTransferee != None {
			r.leadTransferee = None
		}
		// Transfer leadership to self, there will be noop
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		// Transfer leadership to non-existing node, there will be noop.
		return
	}
	r.leadTransferee = m.From
	// Check the qualification
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		// Log catch up, send timeout directly
		r.sendTimeout(m.From)
		return
	}
	// transfer does not complete after about an election timeout
	// the transfer aborted and resume acception client operation
	r.electionElapsed = 0
	r.sendAppend(m.From)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		// Have been added
		return
	}
	r.Prs[id] = &Progress{
		Match: 0,
		Next:  r.RaftLog.LastIndex() + 1,
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		// Have been removed
		return
	}
	delete(r.Prs, id)
	if r.id != id {
		if r.State == StateLeader {
			// Remove node reduces the quorum requirements
			r.updateCommitted()
		}
	}
}

// Avoid more than one conf change
func (r *Raft) updatePengingConfIdx() {
	r.PendingConfIndex = 0
	if len(r.RaftLog.entries) == 0 {
		return
	}
	if ents, err := r.RaftLog.Entries(r.RaftLog.applied+1, r.RaftLog.LastIndex()+1); err != nil {
		panic(err.Error())
	} else {
		for _, ent := range ents {
			if ent.EntryType == pb.EntryType_EntryConfChange {
				r.PendingConfIndex = ent.Index
			}
		}
	}
	log.Printf("r.PendingConfIndex = %v", r.PendingConfIndex)
}
