cnt=$1
for ((i=0;i<cnt;i++))
do
go test -count=1 -v -run ^TestPeerStorageTerm$ github.com/pingcap-incubator/tinykv/kv/raftstore
go test -count=1 -v -run ^TestPeerStorageClearMeta$ github.com/pingcap-incubator/tinykv/kv/raftstore
go test -count=1 -v -run ^TestPeerStorageEntries$ github.com/pingcap-incubator/tinykv/kv/raftstore
go test -count=1 -v -run ^TestPeerStorageAppend$ github.com/pingcap-incubator/tinykv/kv/raftstore
go test -count=1 -v -run ^TestBasic2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestConcurrent2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestUnreliable2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestOnePartition2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestManyPartitionsOneClient2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestManyPartitionsManyClients2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestPersistOneClient2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestPersistConcurrent2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestPersistConcurrentUnreliable2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestPersistPartition2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -v -run ^TestPersistPartitionUnreliable2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
done