package impl

import (
	"iter"
	"maps"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/comms/persistance"
	"analyzer/workers"
	"analyzer/workers/join/config"

	"github.com/op/go-logging"
)

const (
	LEFT = iota + 1
	RIGHT
)

const OUT_OF_ORDER_FILENAME = "out-of-order"
const READING_RIGHT_FILENAME = "reading-right"
const LEFT_PERSISTOR_DIRNAME = "left-persistor"
const RIGHT_PERSISTOR_DIRNAME = "right-persistor"

type Join struct {
	*workers.Worker
	Con            *config.JoinConfig
	leftPersistor  persistance.Persistor
	rightPersistor persistance.Persistor

	// Persisted
	readingRight map[int]struct{}
}

func (w *Join) tryRecover() error {
	persistedFiles, err := w.rightPersistor.Recover()
	if err != nil {
		return err
	}

	for pf := range persistedFiles {
		clientId := pf.ClientId
		if pf.FileName == READING_RIGHT_FILENAME {
			w.readingRight[clientId] = struct{}{}
		}
	}

	return nil
}

func New(con *config.JoinConfig, log *logging.Logger) (*Join, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	w := &Join{
		Worker:         base,
		Con:            con,
		readingRight:   make(map[int]struct{}),
		leftPersistor:  persistance.New(LEFT_PERSISTOR_DIRNAME, con.InputCopies[0], log),
		rightPersistor: persistance.New(RIGHT_PERSISTOR_DIRNAME, con.InputCopies[1], log),
	}

	if err := w.tryRecover(); err != nil {
		return nil, err
	}

	return w, nil
}

func joinFieldMaps(left map[string]string, right map[string]string) map[string]string {
	joined := make(map[string]string, len(left)+len(right))
	maps.Copy(joined, left)
	maps.Copy(joined, right)
	return joined
}

func keyHash(str string) string {
	return str
}

func (w *Join) encode(fieldMaps []map[string]string) []byte {
	return comms.NewBatch(fieldMaps).EncodeForPersistance()
}

func (w *Join) decode(state []byte) (iter.Seq[map[string]string], error) {
	batch, err := comms.DecodeBatch(state)
	if err != nil {
		return nil, err
	}

	return func(yield func(map[string]string) bool) {
		for _, fieldMap := range batch.FieldMaps {
			if !yield(fieldMap) {
				return
			}
		}
	}, nil
}

func handleLeft(w *Join, id middleware.DelId, data []byte) error {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Criticalf("failed to decode batch: %v", err)
		return err
	}

	shardKeys := []string{w.Con.LeftKey}
	shards, err := comms.Shard(batch.FieldMaps, shardKeys, keyHash)
	if err != nil {
		return err
	}

	clientId := id.ClientId

	for k, partialShard := range shards {
		partialEncoded := w.encode(partialShard)
		pf, err := w.leftPersistor.Load(clientId, k)
		exists := err == nil
		if !exists {
			if err := w.leftPersistor.Store(id, k, partialEncoded); err != nil {
				return err
			}
			continue
		}

		header := pf.Header
		if header.IsDup(id) {
			continue
		}

		newState := append(pf.State, partialEncoded...)
		if err := w.leftPersistor.Store(id, k, newState, header); err != nil {
			return err
		}
	}

	return nil
}

func handleRight(w *Join, id middleware.DelId, data []byte) error {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatalf("failed to decode batch: %v", err)
	}

	shardKeys := []string{w.Con.RightKey}
	shards, err := comms.Shard(batch.FieldMaps, shardKeys, keyHash)
	if err != nil {
		return err
	}

	clientId := id.ClientId
	responseFieldMaps := make([]map[string]string, 0)

	for k, shard := range shards {
		pf, err := w.leftPersistor.Load(clientId, k)
		exists := err == nil
		if !exists {
			continue
		}

		decodedLefts, err := w.decode(pf.State)
		if err != nil {
			continue
		}

		for left := range decodedLefts {
			for _, right := range shard {
				joined := joinFieldMaps(left, right)
				responseFieldMaps = append(responseFieldMaps, joined)
			}
		}
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return nil
}

func handleOutOfOrder(w *Join, id middleware.DelId, data []byte) error {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Criticalf("failed to decode batch: %v", err)
		return err
	}

	replicaId := id.ReplicaId
	clientId := id.ClientId
	seq := id.Seq

	encoded := w.encode(batch.FieldMaps)
	pf, err := w.rightPersistor.Load(clientId, OUT_OF_ORDER_FILENAME)

	exists := err == nil
	if !exists {
		return w.rightPersistor.Store(id, OUT_OF_ORDER_FILENAME, encoded)
	}

	header := pf.Header
	if seq <= header.Seqs[replicaId] {
		return nil
	}

	newState := append(pf.State, encoded...)
	return w.rightPersistor.Store(id, OUT_OF_ORDER_FILENAME, newState)
}

func (w *Join) Run() error {
	return w.Worker.Run(w)
}

func (w *Join) Batch(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId

	id := del.Id()
	body := del.Body
	if _, ok := w.readingRight[clientId]; !ok && qId == LEFT {
		// Reading left and given data is from LEFT queue
		if err := handleLeft(w, id, body); err != nil {
			w.Log.Errorf("error while handling batch in left side: %v", err)
		}

	} else if _, ok := w.readingRight[clientId]; ok && qId == RIGHT {
		// Reading right and given data is from RIGHT queue
		if err := handleRight(w, id, body); err != nil {
			w.Log.Errorf("error while handling batch in right side: %v", err)
		}

	} else if _, ok := w.readingRight[clientId]; !ok && qId == RIGHT {
		// Reading left and given data is from RIGHT queue
		if err := handleOutOfOrder(w, id, body); err != nil {
			w.Log.Errorf("error while handling batch out of order: %v", err)
		}

	} else {
		// Reading right and given data is from LEFT queue
		w.Log.Errorf("unexpected left side message when expecting right for client %d in qId %d", clientId, qId)
	}
}

func (w *Join) Eof(qId int, del middleware.Delivery) {
	id := del.Id()
	clientId := id.ClientId
	body := del.Body

	if _, ok := w.readingRight[clientId]; !ok {
		w.readingRight[clientId] = struct{}{}

		pf, err := w.rightPersistor.Load(clientId, OUT_OF_ORDER_FILENAME)
		exists := err == nil
		if exists {
			del.Body = pf.State
			del.Headers.Kind = comms.BATCH
			w.Batch(RIGHT, del)
		}

		w.rightPersistor.Store(id, READING_RIGHT_FILENAME, []byte{})
		return
	}

	_, err := w.rightPersistor.LoadHeader(clientId, READING_RIGHT_FILENAME)
	exists := err == nil

	if exists {
		eof := comms.DecodeEof(body)
		if err := w.Mailer.PublishEof(eof, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}
}

func (w *Join) flush(clientId int) {
	delete(w.readingRight, clientId)
	if err := w.leftPersistor.Flush(clientId); err != nil {
		w.Log.Errorf("failed to flush left inner state for client %d: %v", clientId, err)
	}
	if err := w.rightPersistor.Flush(clientId); err != nil {
		w.Log.Errorf("failed to flush right inner state for client %d: %v", clientId, err)
	}
}

func (w *Join) Flush(qId int, del middleware.Delivery) {
	if qId != RIGHT {
		return
	}

	clientId := del.Headers.ClientId
	body := del.Body

	w.flush(clientId)
	flush := comms.DecodeFlush(body)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Join) purge() {
	w.readingRight = make(map[int]struct{})
	if err := w.leftPersistor.Purge(); err != nil {
		w.Log.Errorf("failed to purge left inner state: %v", err)
	}
	if err := w.rightPersistor.Purge(); err != nil {
		w.Log.Errorf("failed ot purge right inner state: %v", err)
	}
}

func (w *Join) Purge(qId int, del middleware.Delivery) {
	if qId != RIGHT {
		return
	}

	body := del.Body

	w.purge()
	purge := comms.DecodePurge(body)
	if err := w.Mailer.PublishPurge(purge); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
