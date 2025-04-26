package impl

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"maps"
	"os"

	"analyzer/comms"
	"analyzer/workers"
	"analyzer/workers/join/config"

	"github.com/op/go-logging"
)

const (
	STORE = iota
	LOAD
)

type tuple struct {
	kind   int
	fields []map[string]string
}

type Join struct {
	*workers.Worker
	Con         *config.JoinConfig
	readingLeft bool
	chans       []chan tuple
}

func New(con *config.JoinConfig, log *logging.Logger) (*Join, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	w := &Join{
		Worker:      base,
		Con:         con,
		readingLeft: true,
		chans:       nil,
	}

	chans := make([]chan tuple, 0, con.NShards)
	for i := range con.NShards {
		ch := make(chan tuple)
		chans = append(chans, ch)
		go func(i int) {
			if err := spawn_file_persistor(w, ch, i); err != nil {
				w.Log.Errorf("error in spawn_file_persistor: %v", err)
			}
		}(i)
	}

	w.chans = chans
	return w, nil
}

func store(writer *bufio.Writer, fieldMaps []map[string]string) error {
	data := comms.NewBatch(fieldMaps).EncodeForPersistance()
	writer.Write(data)
	return writer.Flush()
}

func joinFieldMaps(left map[string]string, right map[string]string) map[string]string {
	if len(left) > len(right) {
		maps.Copy(left, right)
		return left
	} else {
		maps.Copy(right, left)
		return right
	}
}

func load(w *Join, fp *os.File, fieldMaps []map[string]string) ([]map[string]string, error) {
	responseFieldMaps := make([]map[string]string, 0)
	fp.Seek(0, 0)
	reader := bufio.NewReader(fp)

	for {
		line, err := reader.ReadBytes('\n')
		line = bytes.TrimSpace(line)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		left, err := comms.DecodeLine(line)
		if err != nil {
			w.Log.Fatalf("failed to decode line: %v", err)
		}

		valueLeft, ok := left[w.Con.LeftKey]
		if !ok {
			w.Log.Errorf("key %v was not found in left side", w.Con.LeftKey)
			continue
		}

		for _, right := range fieldMaps {
			valueRight, ok := right[w.Con.RightKey]
			if !ok {
				w.Log.Errorf("key %v was not found in right side", w.Con.RightKey)
				continue
			}

			if valueLeft == valueRight {
				joined := joinFieldMaps(left, right)
				responseFieldMaps = append(responseFieldMaps, joined)
			}
		}
	}

	return responseFieldMaps, nil
}

func spawn_file_persistor(w *Join, ch chan tuple, i int) error {
	path := fmt.Sprintf("/%d.csv", i)
	fp, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("the file %v could not be created %v", i, err)
	}
	defer fp.Close()

	writer := bufio.NewWriter(fp)
	for tup := range ch {
		switch tup.kind {
		case STORE:
			if err := store(writer, tup.fields); err != nil {
				w.Log.Errorf("there was an error storing in the file: %v", err)
			}

		case LOAD:
			fieldMaps, err := load(w, fp, tup.fields)
			if err != nil {
				w.Log.Errorf("there was an error loading up field maps: %v", err)
			}

			ch <- tuple{LOAD, fieldMaps}
		}
	}

	return os.Remove(path)
}

func (w *Join) Run() error {
	return w.Worker.Run(w)
}

func (w *Join) Batch(data []byte) bool {
	if w.readingLeft {
		if err := handleLeft(w, data); err != nil {
			w.Log.Errorf("error while handling batch in left side")
		}
	} else {
		if err := handleRight(w, data); err != nil {
			w.Log.Errorf("error while handling batch in right side")
		}
	}
	return false
}

func (w *Join) Eof(data []byte) bool {
	if w.readingLeft {
		w.readingLeft = false
	} else {
		// Close channels and files
		for _, ch := range w.chans {
			close(ch)
		}

		// Send Eof
		eof := comms.DecodeEof(data)
		if err := w.PublishEof(eof); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return true
}

func (w *Join) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	w.readingLeft = false
	return true
}

func keyHash(str string, mod int) int {
	var hash uint64 = 5381

	for _, c := range str {
		hash = ((hash << 5) + hash) + uint64(c) // hash * 33 + c
	}

	return int(hash % uint64(mod))
}

func shard(w *Join, fieldMaps []map[string]string, key string) map[int][]map[string]string {
	shards := make(map[int][]map[string]string, w.Con.NShards)

	for _, fieldMap := range fieldMaps {
		key, ok := fieldMap[key]
		if !ok {
			w.Log.Errorf("left key %v was not found in field map", w.Con.LeftKey)
			continue
		}

		shardKey := keyHash(key, w.Con.NShards)
		shards[shardKey] = append(shards[shardKey], fieldMap)
	}

	return shards
}

func handleLeft(w *Join, data []byte) error {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatalf("failed to decode line: %v", err)
	}
	shards := shard(w, batch.FieldMaps, w.Con.LeftKey)

	for i, shard := range shards {
		w.chans[i] <- tuple{STORE, shard}
	}

	return nil
}

func handleRight(w *Join, data []byte) error {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatalf("failed to decode batch: %v", err)
	}
	shards := shard(w, batch.FieldMaps, w.Con.RightKey)

	for i, shard := range shards {
		w.chans[i] <- tuple{LOAD, shard}
	}

	for i := range shards {
		resultTuples := <-w.chans[i]
		responseFieldMaps := resultTuples.fields
		if len(responseFieldMaps) > 0 {
			w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
			batch := comms.NewBatch(responseFieldMaps)
			if err := w.PublishBatch(batch); err != nil {
				w.Log.Errorf("failed to publish message: %v", err)
			}
		}
	}

	return nil
}
