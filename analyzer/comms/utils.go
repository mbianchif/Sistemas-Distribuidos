package comms

import (
	"fmt"
	"io"
	"os"
)

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

func writeAll(w io.Writer, data []byte) error {
	written := 0
	for written < len(data) {
		n, err := w.Write(data[written:])
		if err != nil {
			return err
		}
		written += n
	}

	return nil
}

func AtomicWrite(dirPath, fileName string, data []byte) error {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s/%s.tmp", dirPath, fileName)
	fp, err := os.Create(tmpFileName)
	if err != nil {
		return fmt.Errorf("Couldn't create temp file for path %s/%s: %v", dirPath, fileName, err)
	}
	defer fp.Close()

	if err := writeAll(fp, data); err != nil {
		return fmt.Errorf("Couldn't write to temp file for path %s/%s: %v", dirPath, fileName, err)
	}

	newFileName := fmt.Sprintf("%s/%s.txt", dirPath, fileName)
	return os.Rename(tmpFileName, newFileName)
}
