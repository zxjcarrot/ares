#! /bin/sh
cd mockrpc; go clean;go install;cd ..;
cd raft; go clean; go install; cd ..;
cd shardkv; go clean; go install; cd ..;
cd shardmaster; go clean; go install; cd ..;
cd cmd/shardkv-server; go clean; go build; cd ../..;
cd cmd/master-server; go clean; go build; cd ../..;
