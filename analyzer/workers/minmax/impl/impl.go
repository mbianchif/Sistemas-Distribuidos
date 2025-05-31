package impl

import (
	"bytes"
	"fmt"
	"strconv"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/comms/persistance"
	"analyzer/workers"
	"analyzer/workers/minmax/config"

	"github.com/op/go-logging"
)

const PERSISTOR_FILENAME = "state.txt"

type tuple struct {
	fieldMap map[string]string
	value    float64
}

type MinMax struct {
	*workers.Worker
	Con       *config.MinMaxConfig
	persistor persistance.Persistor

	// Persisted
	mins map[int]tuple
	maxs map[int]tuple
}

func (w *MinMax) tryRecover() error {
	persistedFiles, err := w.persistor.Recover()
	if err != nil {
		return fmt.Errorf("failed to recover persisted files: %v", err)
	}

	for pf := range persistedFiles {
		clientId := pf.ClientId
		state := pf.State

		if err := w.Decode(clientId, state); err != nil {
			return fmt.Errorf("failed to decode state for client %d: %v", clientId, err)
		}
	}

	return nil
}

func New(con *config.MinMaxConfig, log *logging.Logger) (*MinMax, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	w := MinMax{
		Worker:    base,
		Con:       con,
		persistor: persistance.New(log),
		mins:      make(map[int]tuple),
		maxs:      make(map[int]tuple),
	}

	if err := w.tryRecover(); err != nil {
		return nil, err
	}

	return &w, nil
}

func (w *MinMax) Run() error {
	return w.Worker.Run(w)
}

func (w *MinMax) clean(clientId int) {
	delete(w.mins, clientId)
	delete(w.maxs, clientId)
}

func handleMinMax(w *MinMax, clientId int, fieldMap map[string]string) error {
	if _, ok := fieldMap[w.Con.Key]; !ok {
		return fmt.Errorf("key %v was not found in the field map", w.Con.Key)
	}

	value, err := strconv.ParseFloat(fieldMap[w.Con.Key], 64)
	if err != nil {
		return err
	}

	max := w.maxs[clientId]
	min := w.mins[clientId]

	if max.fieldMap == nil {
		max = tuple{fieldMap, value}
	}
	if min.fieldMap == nil {
		min = tuple{fieldMap, value}
	}

	if value > max.value {
		max = tuple{fieldMap, value}
	}
	if value < min.value {
		min = tuple{fieldMap, value}
	}

	w.maxs[clientId] = max
	w.mins[clientId] = min
	return nil
}

func (w *MinMax) Encode(clientId int) []byte {
	buf := bytes.NewBuffer(nil)

	// Write min fieldmap
	if tup, ok := w.mins[clientId]; ok {
		min := comms.NewBatch([]map[string]string{tup.fieldMap}).EncodeForPersistance()
		buf.Write(min)
	}

	// Write max fieldmap
	if tup, ok := w.maxs[clientId]; ok {
		max := comms.NewBatch([]map[string]string{tup.fieldMap}).EncodeForPersistance()
		buf.Write(max)
	}

	return buf.Bytes()
}

func (w *MinMax) Decode(clientId int, state []byte) error {
	lines := bytes.Split(state, []byte("\n"))
	if len(lines) < 2 {
		return fmt.Errorf("state does not contain enough data for client %d", clientId)
	}

	minBatch, err := comms.DecodeBatch(bytes.TrimSpace(lines[0]))
	if err != nil {
		return fmt.Errorf("failed to decode min batch for client %d: %v", clientId, err)
	}
	min := minBatch.FieldMaps[0]

	maxBatch, err := comms.DecodeBatch(bytes.TrimSpace(lines[1]))
	if err != nil {
		return fmt.Errorf("failed to decode max batch for client %d: %v", clientId, err)
	}
	max := maxBatch.FieldMaps[0]

	handleMinMax(w, clientId, min)
	handleMinMax(w, clientId, max)
	return nil
}

func (w *MinMax) Batch(qId int, del middleware.Delivery) {
	body := del.Body
	batch, err := comms.DecodeBatch(body)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}

	// Check for duplicated deliveries
	if w.persistor.IsDup(del) {
		return
	}

	clientId := del.Headers.ClientId
	for _, fieldMap := range batch.FieldMaps {
		err := handleMinMax(w, clientId, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	// Persist once the entire delivery is processed
	state := w.Encode(clientId)
	w.persistor.Store(del.Id(), PERSISTOR_FILENAME, state)
}

func (w *MinMax) Eof(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	responseFieldMaps := []map[string]string{
		w.mins[clientId].fieldMap,
		w.maxs[clientId].fieldMap,
	}

	w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
	batch := comms.NewBatch(responseFieldMaps)
	if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	body := del.Body
	eof := comms.DecodeEof(body)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *MinMax) Flush(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	w.clean(clientId)
	flush := comms.DecodeFlush(body)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
