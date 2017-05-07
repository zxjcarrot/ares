package shardkv

func copyKVMap(m map[string]string) map[string]string {
	res := map[string]string{}
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyReqIdMap(m map[int64]bool) map[int64]bool {
	res := map[int64]bool{}
	for k, v := range m {
		res[k] = v
	}
	return res
}

func copyKVStore(s KVStore) KVStore {
	return KVStore{s.ShardIdx, copyKVMap(s.Store), copyReqIdMap(s.ReqIdMap), s.Version, s.From}
}
