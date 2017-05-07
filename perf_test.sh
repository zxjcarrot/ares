#! /bin/sh
cd examples/1-replica/shardkv-node1;
./shardkv-server  & PID=$!
sleep 1
cd ../../../cmd/shardkvctl;
#cd cmd/shardkvctl;
go test -m localhost:22222:0,localhost:22223:1,localhost:22224:2 -bench . -benchtime 10s;
kill -s INT $PID;

