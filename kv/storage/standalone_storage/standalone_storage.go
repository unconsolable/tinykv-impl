package standalone_storage

import (
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	conf   *config.Config
}

type StandAloneStorageReader struct {
	txn   *badger.Txn
	iters []*engine_util.BadgerIterator
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	// If the requested key doesn't exist; error will not be signalled.
	// While val will be nil
	if err != nil && err != badger.ErrKeyNotFound {
		return nil, err
	}
	return val, nil
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	r.iters = append(r.iters, engine_util.NewCFIterator(cf, r.txn))
	return r.iters[len(r.iters)-1]
}

func (r *StandAloneStorageReader) Close() {
	for _, v := range r.iters {
		v.Close()
	}
	r.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dbPath := s.conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)
	s.engine = engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &StandAloneStorageReader{s.engine.Kv.NewTransaction(false), nil}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writes := engine_util.WriteBatch{}
	writes.Reset()
	for _, v := range batch {
		if v.Value() == nil {
			writes.DeleteCF(v.Cf(), v.Key())
		} else {
			writes.SetCF(v.Cf(), v.Key(), v.Value())
		}
	}
	return writes.WriteToDB(s.engine.Kv)
}
