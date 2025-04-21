package impl

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"workers"
	"workers/join/config"
	"workers/protocol"

	"maps"

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
		go spawn_file_persistor(w, ch, i)
	}

	w.chans = chans
	return w, nil
}

func store(writer *bufio.Writer, fieldMaps []map[string]string) error {
	data := protocol.NewBatch(fieldMaps).EncodeForPersistance()
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
	for _, right := range fieldMaps {
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

			left := protocol.DecodeLine(line)
			valueLeft, ok := left[w.Con.LeftKey]
			if !ok {
				w.Log.Errorf("key %v was not found in left side", w.Con.LeftKey)
				continue
			}

			valueRight, ok := right[w.Con.RightKey]
			if !ok {
				w.Log.Errorf("key %v was not found in right side", w.Con.RightKey)
			}

			if valueLeft == valueRight {
				joined := joinFieldMaps(left, right)
				responseFieldMaps = append(responseFieldMaps, joined)
			}
		}
	}

	// TODO: Remove file
	return responseFieldMaps, nil
}

func spawn_file_persistor(w *Join, ch chan tuple, i int) error {
	path := "/tmp/storage/" + strconv.Itoa(i) + ".csv"
	fp, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("the file %v could not be created %v", i, err)
	}
	defer fp.Close()

	writer := bufio.NewWriter(fp)
	for tup := range ch {
		switch tup.kind {
		case STORE:
			w.Log.Debugf("STORE %v", tup.fields)
			store(writer, tup.fields)

		case LOAD:
			fieldMaps, err := load(w, fp, tup.fields)
			w.Log.Debugf("LOAD %v", tup.fields)
			if err != nil {
				w.Log.Errorf("there was an error loading up field maps %v", err)
				continue
			}

			ch <- tuple{LOAD, fieldMaps}
		}
	}

	return nil
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
		body := protocol.DecodeEof(data).Encode()
		if err := w.Broker.Publish("", body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return true
}

func (w *Join) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}

func keyHash(field string, mod int) int {
	acc := 0
	for _, ch := range field {
		acc += int(ch)
	}
	return acc % mod
}

func shard(w *Join, fieldMaps []map[string]string) map[int][]map[string]string {
	shards := make(map[int][]map[string]string, w.Con.NShards)

	for _, fieldMap := range fieldMaps {
		key, ok := fieldMap[w.Con.LeftKey]
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
	batch := protocol.DecodeBatch(data)
	shards := shard(w, batch.FieldMaps)

	for i, shard := range shards {
		w.chans[i] <- tuple{STORE, shard}
	}

	return nil
}

func handleRight(w *Join, data []byte) error {
	batch := protocol.DecodeBatch(data)
	shards := shard(w, batch.FieldMaps)

	for i, shard := range shards {
		w.chans[i] <- tuple{LOAD, shard}
	}

	for i := range shards {
		resultTuples := <-w.chans[i]
		responseFieldMaps := resultTuples.fields

		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		body := protocol.NewBatch(responseFieldMaps).Encode(w.Con.Select)
		if err := w.Broker.Publish("", body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return nil
}
