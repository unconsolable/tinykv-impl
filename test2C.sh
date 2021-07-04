cnt=$1
for ((i=0;i<cnt;i++))
do
go test -count=1 -v -run ^TestRestoreSnapshot2C$ github.com/pingcap-incubator/tinykv/raft
go test -count=1 -v -run ^TestRestoreIgnoreSnapshot2C$ github.com/pingcap-incubator/tinykv/raft
go test -count=1 -v -run ^TestProvideSnap2C$ github.com/pingcap-incubator/tinykv/raft
go test -count=1 -v -run ^TestRestoreFromSnapMsg2C$ github.com/pingcap-incubator/tinykv/raft
go test -count=1 -v -run ^TestSlowNodeRestore2C$ github.com/pingcap-incubator/tinykv/raft
go test -count=1 -v -run ^TestRawNodeRestartFromSnapshot2C$ github.com/pingcap-incubator/tinykv/raft
go test -count=1 -v -run ^TestOneSnapshot2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestSnapshotRecover2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestSnapshotRecoverManyClients2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestSnapshotUnreliable2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestSnapshotUnreliableRecover2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestSnapshotUnreliableRecoverConcurrentPartition2C$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
done