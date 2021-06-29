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

	// Peers id
	peers []uint64
	// half election timeout, used for reset electionElapsed
	halfElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// Recover HardState
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}
	// Init Raft state
	ret := &Raft{
		id:                  c.ID,
		electionTimeout:     2 * c.ElectionTick,
		heartbeatTimeout:    c.HeartbeatTick,
		Term:                hardState.Term,
		Vote:                hardState.Vote,
		State:               StateFollower,
		peers:               c.peers,
		halfElectionTimeout: c.ElectionTick,
	}
	// Recover RaftLog
	ret.RaftLog = newLog(c.Storage)
	ret.RaftLog.committed = hardState.Commit
	ret.RaftLog.applied = c.Applied
	// Switch to follower
	ret.becomeFollower(0, None)
	// Set random seed
	rand.Seed(time.Now().UnixNano())
	return ret
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	reqMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, reqMsg)
	DPrintf("%d send heartbeat to %d\n", r.id, to)
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

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// r.msgs = append(r.msgs, pb.Message{
			// 	MsgType: pb.MessageType_MsgBeat,
			// })
			r.bcastHeartbeat()
		}
		DPrintf("%d add tick for heartbeat...\n", r.id)
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			// r.msgs = append(r.msgs, pb.Message{
			// 	MsgType: pb.MessageType_MsgHup,
			// })
			r.becomeCandidate()
			r.campaign()
		}
		DPrintf("%d add tick for election...\n", r.id)
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	// Reset term, leader, election timeout, vote record
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	// Switch state to follower
	r.State = StateFollower
	DPrintf("%d become Follower, term %v\n", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// Update term, vote to itself, reset election timeout
	r.Term++
	r.Vote = r.id
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	// Reset votes
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	// Switch state to candidate
	r.State = StateCandidate
	DPrintf("%d become Candidate at Term %v\n", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// Reset heartbeat tick
	r.heartbeatElapsed = 0
	// Init progress
	r.Prs = make(map[uint64]*Progress)
	for _, v := range r.peers {
		r.Prs[v] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
	// Propose a noop entry on its term
	noOp := pb.Entry{}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		To:      r.id,
		Term:    r.Term,
		Entries: []*pb.Entry{&noOp},
	})
	// Switch state to leader
	r.Lead = r.id
	r.State = StateLeader
	DPrintf("%d become Leader\n", r.id)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// Check the terms of node and incoming message to prevent stale log entries
	if IsResponseMsg(m.MsgType) && m.Term < r.Term {
		return nil
	}
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
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgRequestVoteResponse:
		return nil
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
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgHeartbeatResponse:
		return nil
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
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgRequestVoteResponse:
		return nil
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
	if cnt*2 > len(r.peers) {
		// Win majority
		r.becomeLeader()
		return
	}
	// Not enough, start campaign
	for _, v := range r.peers {
		if v == r.id {
			continue
		}
		r.sendRequestVote(v)
	}
}

// appendEntry handle MsgPropose message
func (r *Raft) appendEntry(m pb.Message) {
	// Your Code Here (2A).
}

// bcastAppend send entries to peers
func (r *Raft) bcastAppend() {
	// Your Code Here (2A).
}

// bcastHeartbeat send heartbeat to peers
func (r *Raft) bcastHeartbeat() {
	// Your Code Here (2A).
	for _, v := range r.peers {
		if v == r.id {
			continue
		}
		r.sendHeartbeat(v)
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
	// Reply false if term < currentTerm
	if m.Term < r.Term {
		r.msgs = append(r.msgs, respMsg)
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
		r.msgs = append(r.msgs, respMsg)
		return
	}
	// Raft.Vote is null, only vote once
	if r.Vote == None || r.Vote == m.From {
		DPrintf("%d grand vote to %d\n", r.id, m.From)
		r.Vote = m.From
		respMsg.Reject = false
		r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	}
	r.msgs = append(r.msgs, respMsg)
}

// handleRequestVote handle RequestVote RPC response
func (r *Raft) handleRequestVoteResp(m pb.Message) {
	// Your Code Here (2A).
	if m.Reject {
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.votes[m.From] = false
		}
	} else {
		// Record and count vote
		r.votes[m.From] = true
		aye, nay := 0, 0
		for _, v := range r.votes {
			if v {
				aye++
			} else {
				nay++
			}
		}
		if aye*2 > len(r.peers) {
			// Win majority
			r.becomeLeader()
		}
		if nay*2 > len(r.peers) {
			// Lose majority
			r.becomeFollower(r.Term, None)
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// Reply false if Term < currentTerm
	respMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Reject:  false,
	}
	// Reply false if Term < Raft.Term
	if m.Term < r.Term {
		respMsg.Reject = true
		r.msgs = append(r.msgs, respMsg)
		return
	}
	// If RPC request or response contains term T > currentTerm,
	// or as large as the candidate’s current term
	// set currentTerm = T, convert to follower
	if m.Term > r.Term || (m.Term == r.Term && r.State == StateCandidate) {
		r.becomeFollower(m.Term, m.From)
	}
	respMsg.Term = r.Term
	// Reset election timer
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	// ---------------------------
	// TODO: compare and append
	// ---------------------------
	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last *new* entry)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
	}
	r.msgs = append(r.msgs, respMsg)
}

// handleAppendEntries handle AppendEntries RPC response
func (r *Raft) handleAppendEntriesResp(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// Reply false if Term < currentTerm
	respMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    m.To,
		Term:    r.Term,
		Reject:  false,
	}
	// Reply false if Term < Raft.Term
	if m.Term < r.Term {
		respMsg.Reject = true
		r.msgs = append(r.msgs, respMsg)
		return
	}
	// If RPC request or response contains term T > currentTerm,
	// or as large as the candidate’s current term
	// set currentTerm = T, convert to follower
	if m.Term > r.Term || (m.Term == r.Term && r.State == StateCandidate) {
		r.becomeFollower(m.Term, m.From)
	}
	respMsg.Term = r.Term
	// Reset election timer
	r.electionElapsed = rand.Intn(r.halfElectionTimeout) + 1
	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last *new* entry)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(r.RaftLog.LastIndex(), m.Commit)
	}
	r.msgs = append(r.msgs, respMsg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
