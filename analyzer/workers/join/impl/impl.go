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

func New(con *config.JoinConfig, log *logging.Logger) (*Join, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	w := &Join{
		Worker:         base,
		Con:            con,
		readingRight:   make(map[int]struct{}),
		leftPersistor:  persistance.New(LEFT_PERSISTOR_DIRNAME, log),
		rightPersistor: persistance.New(RIGHT_PERSISTOR_DIRNAME, log),
	}

	return w, nil
}

func (w *Join) clean(clientId int) {
	delete(w.readingRight, clientId)
	// 1. Pedirle al persistor que borre lo de este cliente
}

func joinFieldMaps(left map[string]string, right map[string]string) map[string]string {
	joined := make(map[string]string, len(left)+len(right))
	maps.Copy(joined, left)
	maps.Copy(joined, right)
	return joined
}

func keyHash(str string, _ int) string {
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

	shards, err := comms.Shard(batch.FieldMaps, w.Con.LeftKey, w.Con.NShards, keyHash)
	if err != nil {
		return err
	}

	replicaId := id.ReplicaId
	clientId := id.ClientId
	seq := id.Seq

	for k, partialShard := range shards {
		lastReplicaId, lastSeq, state, err := w.leftPersistor.Load(clientId, k)
		partialEncoded := w.encode(partialShard)

		exists := err == nil
		if !exists {
			if err := w.leftPersistor.Store(id, k, partialEncoded); err != nil {
				return err
			}
			continue
		}

		if seq <= lastSeq && replicaId == lastReplicaId {
			continue
		}

		newState := append(state, partialEncoded...)
		if err := w.leftPersistor.Store(id, k, newState); err != nil {
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
	shards, err := comms.Shard(batch.FieldMaps, w.Con.RightKey, w.Con.NShards, keyHash)
	if err != nil {
		return err
	}

	clientId := id.ClientId
	responseFieldMaps := make([]map[string]string, 0)

	for k, shard := range shards {
		_, _, state, err := w.leftPersistor.Load(clientId, k)

		exists := err == nil
		if !exists {
			continue
		}

		decodedLefts, err := w.decode(state)
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
	lastReplicaId, lastSeq, state, err := w.rightPersistor.Load(clientId, OUT_OF_ORDER_FILENAME)

	exists := err == nil
	if !exists {
		w.rightPersistor.Store(id, OUT_OF_ORDER_FILENAME, encoded)
		return nil
	}

	if seq <= lastSeq && replicaId == lastReplicaId {
		return nil
	}

	newState := append(state, encoded...)
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

		_, _, state, err := w.rightPersistor.Load(clientId, OUT_OF_ORDER_FILENAME)
		exists := err == nil
		if exists {
			del.Body = state
			del.Headers.Kind = comms.BATCH
			w.Batch(RIGHT, del)
		}

		w.rightPersistor.Store(id, READING_RIGHT_FILENAME, []byte{})
	} else if !w.rightPersistor.IsDup(del) {
		eof := comms.DecodeEof(body)
		if err := w.Mailer.PublishEof(eof, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}
}

func (w *Join) Flush(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	w.clean(clientId)
	flush := comms.DecodeFlush(body)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
