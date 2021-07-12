cnt=$1
for ((i=0;i<cnt;i++))
do
go test -count=1 -v -run ^TestTransferLeader3B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestBasicConfChange3B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestConfChangeRecover3B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestConfChangeRecoverManyClients3B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestConfChangeUnreliable3B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestConfChangeUnreliableRecover3B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestConfChangeSnapshotUnreliableRecover3B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
done
