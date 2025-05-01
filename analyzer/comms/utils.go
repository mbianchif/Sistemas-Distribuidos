package comms

import "fmt"

func keyHash(str string, mod int) int {
	var hash uint64 = 5381

	for _, c := range str {
		hash = ((hash << 5) + hash) + uint64(c) // hash * 33 + c
	}

	return int(hash % uint64(mod))
}

func Shard(fieldMaps []map[string]string, key string, r int) (map[int][]map[string]string, error) {
	shards := make(map[int][]map[string]string, r)

	for _, fieldMap := range fieldMaps {
		key, ok := fieldMap[key]
		if !ok {
			return nil, fmt.Errorf("key %v was not found in field map while sharding", key)
		}

		shardKey := keyHash(key, r)
		shards[shardKey] = append(shards[shardKey], fieldMap)
	}

	return shards, nil
}
