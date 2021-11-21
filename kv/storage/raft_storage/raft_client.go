package raft_storage

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// raftConn represent a gRPC connection to other store.
type raftConn struct {
	streamMu sync.Mutex
	stream   tinykvpb.TinyKv_RaftClient
	ctx      context.Context
	cancel   context.CancelFunc
}

// newRaftConn construct a new connection to other store.
func newRaftConn(addr string, cfg *config.Config) (*raftConn, error) {
	cc, err := grpc.Dial(addr, grpc.WithInsecure(),
		grpc.WithInitialWindowSize(2*1024*1024),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                3 * time.Second,
			Timeout:             60 * time.Second,
			PermitWithoutStream: true,
		}))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := tinykvpb.NewTinyKvClient(cc).Raft(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	return &raftConn{
		stream: stream,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (c *raftConn) Stop() {
	c.cancel()
}

// Send send Raft Message to other NODE
func (c *raftConn) Send(msg *raft_serverpb.RaftMessage) error {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	return c.stream.Send(msg)
}

// RaftClient manage connection to other store, it is
// responsible for sending RaftMessage
type RaftClient struct {
	config *config.Config
	sync.RWMutex
	conns map[string]*raftConn
	// storeID -> addr
	addrs map[uint64]string
}

func newRaftClient(config *config.Config) *RaftClient {
	return &RaftClient{
		config: config,
		conns:  make(map[string]*raftConn),
		addrs:  make(map[uint64]string),
	}
}

// getConn get a connection or construct a new connection to other NODE
func (c *RaftClient) getConn(addr string, regionID uint64) (*raftConn, error) {
	c.RLock()
	conn, ok := c.conns[addr]
	if ok {
		c.RUnlock()
		return conn, nil
	}
	c.RUnlock()
	newConn, err := newRaftConn(addr, c.config)
	if err != nil {
		return nil, err
	}
	c.Lock()
	defer c.Unlock()
	if conn, ok := c.conns[addr]; ok {
		newConn.Stop()
		return conn, nil
	}
	c.conns[addr] = newConn
	return newConn, nil
}

func (c *RaftClient) Send(storeID uint64, addr string, msg *raft_serverpb.RaftMessage) error {
	conn, err := c.getConn(addr, msg.GetRegionId())
	if err != nil {
		return err
	}
	err = conn.Send(msg)
	if err == nil {
		return nil
	}

	log.Error("raft client failed to send")
	c.Lock()
	defer c.Unlock()
	conn.Stop()
	delete(c.conns, addr)
	if oldAddr, ok := c.addrs[storeID]; ok && oldAddr == addr {
		delete(c.addrs, storeID)
	}
	return err
}

func (c *RaftClient) GetAddr(storeID uint64) string {
	c.RLock()
	defer c.RUnlock()
	v, _ := c.addrs[storeID]
	return v
}

func (c *RaftClient) InsertAddr(storeID uint64, addr string) {
	c.Lock()
	defer c.Unlock()
	c.addrs[storeID] = addr
}

func (c *RaftClient) Flush() {
	// Not support BufferHint
}
