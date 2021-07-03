cnt=$1
for ((i=0;i<cnt;i++))
do
# go test -count=1 -run ^TestBasic2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestConcurrent2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestUnreliable2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
go test -count=1 -run ^TestOnePartition2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestManyPartitionsOneClient2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestManyPartitionsManyClients2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestPersistOneClient2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestPersistConcurrent2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestPersistConcurrentUnreliable2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestPersistPartition2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
# go test -count=1 -run ^TestPersistPartitionUnreliable2B$ github.com/pingcap-incubator/tinykv/kv/test_raftstore
done