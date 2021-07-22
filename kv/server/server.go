package server

import (
	"context"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}
	defer reader.Close()
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil && err != badger.ErrKeyNotFound {
		return &kvrpcpb.RawGetResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawGetResponse{Value: value, NotFound: value == nil}, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	err := server.storage.Write(req.Context, []storage.Modify{{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}}})
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	err := server.storage.Write(req.Context, []storage.Modify{{Data: storage.Put{Key: req.Key, Cf: req.Cf}}})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, err
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
	}
	defer reader.Close()
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	kvs := make([]*kvrpcpb.KvPair, 0)
	for i := 0; i < int(req.Limit) && iter.Valid(); i++ {
		value, err := iter.Item().Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{Error: err.Error()}, err
		}
		curkv := &kvrpcpb.KvPair{Key: iter.Item().KeyCopy(nil), Value: value}
		kvs = append(kvs, curkv)
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	// Get Reader via Storage
	reader, err := server.storage.Reader(req.Context)
	resp := &kvrpcpb.GetResponse{}
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	// Check if there is a lock, if it is return and try again later
	lk, err := txn.GetLock(req.Key)
	if err != nil {
		panic(err.Error())
	}
	if lk != nil && lk.Ts <= req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lk.Primary,
				LockVersion: lk.Ts,
				Key:         req.Key,
				LockTtl:     lk.Ttl,
			},
			Retryable: "retry",
		}
		return resp, nil
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		panic(err.Error())
	}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	// Latch the keys
	resp := &kvrpcpb.PrewriteResponse{}
	keysToLatch := make([][]byte, 0)
	for _, mut := range req.Mutations {
		keysToLatch = append(keysToLatch, mut.Key)
	}
	if wg := server.Latches.AcquireLatches(keysToLatch); wg != nil {
		// Keys has latched, return
		resp.Errors = []*kvrpcpb.KeyError{{
			Retryable: "retry",
		}}
		return resp, nil
	}
	defer server.Latches.ReleaseLatches(keysToLatch)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	for _, mut := range req.Mutations {
		// Check if there is a lock, if it is return and try again later
		lk, err := txn.GetLock(mut.Key)
		if err != nil {
			panic(err.Error())
		}
		if lk != nil && lk.Ts <= req.StartVersion {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lk.Primary,
					LockVersion: lk.Ts,
					Key:         mut.Key,
					LockTtl:     lk.Ttl,
				},
			})
			continue
		}
		// Check the most recent write, if there is a newer version, report write conflict
		_, commitTs, err := txn.MostRecentWrite(mut.Key)
		if err != nil {
			panic(err.Error())
		}
		if txn.StartTS <= commitTs {
			resp.Errors = append(resp.Errors, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    txn.StartTS,
					ConflictTs: commitTs,
					Key:        mut.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		// Put Lock and Value
		txn.PutLock(mut.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mut.Op),
		})
		switch mut.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(mut.Key, mut.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(mut.Key)
		}
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	// Latch the keys
	resp := &kvrpcpb.CommitResponse{}
	if wg := server.Latches.AcquireLatches(req.Keys); wg != nil {
		// Keys has latched, return
		resp.Error = &kvrpcpb.KeyError{
			Retryable: "retry",
		}
		return resp, nil
	}
	defer server.Latches.ReleaseLatches(req.Keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	resp.Error = server.CommitKeys(req.Keys, txn, req.CommitVersion)
	if resp.Error == nil {
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
			}
		}
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.Version)
	iter, cnt := mvcc.NewScanner(req.StartKey, txn), uint32(0)
	for cnt < req.Limit {
		key, val, err := iter.Next()
		if key == nil && val == nil {
			break
		}
		var kvp *kvrpcpb.KvPair
		if err != nil {
			kvp = &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{},
			}
		} else {
			kvp = &kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			}
		}
		resp.Pairs = append(resp.Pairs, kvp)
		cnt++
	}
	iter.Close()
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, nil
	}
	defer reader.Close()
	// Check lock and commit status of primary key
	txn := mvcc.NewMvccTxn(reader, req.LockTs)
	lk, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		panic(err.Error())
	}
	if lk != nil {
		if lk.Ts != req.LockTs {
			return resp, nil
		}
		resp.LockTtl = lk.Ttl
	} else {
		write, commit, err := txn.MostRecentWrite(req.PrimaryKey)
		if err != nil {
			panic(err.Error())
		}
		if write != nil {
			if write.Kind == mvcc.WriteKindRollback {
				resp.LockTtl = 0
				resp.CommitVersion = 0
			} else {
				resp.CommitVersion = commit
			}
			return resp, nil
		}
	}
	if lk == nil {
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	} else if mvcc.PhysicalTime(lk.Ts)+lk.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		// Expired
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		lks, err := mvcc.AllLocksForTxn(txn)
		if err != nil {
			panic(err.Error())
		}
		for _, lk := range lks {
			txn.DeleteLock(lk.Key)
			txn.DeleteValue(lk.Key)
		}
	} else {
		return resp, nil
	}
	writeRollback := &mvcc.Write{
		StartTS: req.LockTs,
		Kind:    mvcc.WriteKindRollback,
	}
	txn.PutWrite(req.PrimaryKey, req.LockTs, writeRollback)
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	if wg := server.Latches.AcquireLatches(req.Keys); wg != nil {
		// Keys has latched, return
		resp.Error = &kvrpcpb.KeyError{
			Retryable: "retry",
		}
		return resp, nil
	}
	defer server.Latches.ReleaseLatches(req.Keys)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	resp.Error = server.RollbackKeys(req.Keys, txn)
	if resp.Error == nil {
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
			}
		}
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
		}
		return resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.StartVersion)
	lks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		panic(err.Error())
	}
	var keys [][]byte
	for _, lk := range lks {
		keys = append(keys, lk.Key)
	}
	if wg := server.Latches.AcquireLatches(keys); wg != nil {
		// Keys has latched, return
		resp.Error = &kvrpcpb.KeyError{
			Retryable: "retry",
		}
		return resp, nil
	}
	defer server.Latches.ReleaseLatches(keys)
	if req.CommitVersion == 0 {
		// Rollback
		resp.Error = server.RollbackKeys(keys, txn)
	} else {
		// Commit
		resp.Error = server.CommitKeys(keys, txn, req.CommitVersion)
	}
	if resp.Error == nil {
		if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
			}
		}
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}

func (server *Server) RollbackKeys(keys [][]byte, txn *mvcc.MvccTxn) *kvrpcpb.KeyError {
	for _, key := range keys {
		lk, err := txn.GetLock(key)
		if err != nil {
			panic(err.Error())
		}
		if lk == nil {
			write, _, err := txn.MostRecentWrite(key)
			if err != nil {
				panic(err.Error())
			}
			if write != nil {
				// Committed, can not roll back
				if write.Kind != mvcc.WriteKindRollback {
					return &kvrpcpb.KeyError{
						Abort: "abort",
					}
				}
				return nil
			}
		}
		if lk != nil && lk.Ts == txn.StartTS {
			txn.DeleteValue(key)
			txn.DeleteLock(key)
		}
		writeRollback := &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		}
		txn.PutWrite(key, txn.StartTS, writeRollback)
	}
	return nil
}

func (server *Server) CommitKeys(keys [][]byte, txn *mvcc.MvccTxn, commitTs uint64) *kvrpcpb.KeyError {
	for _, key := range keys {
		lk, err := txn.GetLock(key)
		if err != nil {
			panic(err.Error())
		}
		if lk == nil {
			continue
		}
		if lk.Ts != txn.StartTS {
			return &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: lk.Primary,
					LockVersion: lk.Ts,
					Key:         key,
					LockTtl:     lk.Ttl,
				},
				Retryable: "retry",
			}
		}
		write := &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lk.Kind,
		}
		txn.PutWrite(key, commitTs, write)
		txn.DeleteLock(key)
	}
	return nil
}
