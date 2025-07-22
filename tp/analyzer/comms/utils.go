package comms

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
)

const SEP = "<|>"

func Shard[T comparable](fieldMaps []map[string]string, shardKeys []string, hash func(str string) T) (map[T][]map[string]string, error) {
	shards := make(map[T][]map[string]string)

	for _, fieldMap := range fieldMaps {
		keys := make([]string, 0, len(shardKeys))
		for _, key := range shardKeys {
			field, ok := fieldMap[key]
			if !ok {
				return nil, fmt.Errorf("key %v was not found in field map while sharding", key)
			}
			keys = append(keys, field)
		}

		compKey := strings.Join(keys, SEP)
		shardKey := hash(compKey)
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

	if err := fp.Sync(); err != nil {
		return fmt.Errorf("Coudn't sync temp file for path %s/%s: %v", dirPath, fileName, err)
	}

	newFileName := fmt.Sprintf("%s/%s", dirPath, fileName)
	return os.Rename(tmpFileName, newFileName)
}

func IndexN(s []byte, sep rune, n int) int {
	found := 0
	f := func(r rune) bool {
		if r == sep {
			found++
			if found == n {
				return true
			}
		}
		return false
	}
	return bytes.IndexFunc(s, f)
}
