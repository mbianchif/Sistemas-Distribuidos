package impl

import (
	"bytes"
	"sort"
	"strconv"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/comms/persistance"
	"analyzer/workers"
	"analyzer/workers/top/config"

	"github.com/op/go-logging"
)

const PERSISTOR_FILENAME = "state"

type tuple struct {
	value    float64
	fieldMap map[string]string
}

type Top struct {
	*workers.Worker
	Con       *config.TopConfig
	persistor persistance.Persistor

	// Persisted
	tops map[int][]tuple
}

func New(con *config.TopConfig, log *logging.Logger) (*Top, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	return &Top{
		Worker:    base,
		Con:       con,
		persistor: persistance.New(log),
		tops:      make(map[int][]tuple),
	}, nil
}

func (w *Top) Run() error {
	return w.Worker.Run(w)
}

func (w *Top) clean(clientId int) {
	delete(w.tops, clientId)
}

func handleTop(w *Top, clientId int, fieldMap map[string]string) error {
	value, err := strconv.ParseFloat(fieldMap[w.Con.Key], 64)
	if err != nil {
		return err
	}

	top := append(w.tops[clientId], tuple{value, fieldMap})

	sort.Slice(top, func(i, j int) bool {
		return top[i].value > top[j].value
	})

	if len(top) > w.Con.Amount {
		top = top[:w.Con.Amount]
	}

	w.tops[clientId] = top
	return nil
}

func (w *Top) Encode(clientId int) []byte {
	buf := bytes.NewBuffer(nil)

	fieldMaps := make([]map[string]string, 0, len(w.tops[clientId]))
	for _, tup := range w.tops[clientId] {
		fieldMaps = append(fieldMaps, tup.fieldMap)
	}

	// Write the field maps to the buffer
	batch := comms.NewBatch(fieldMaps)
	buf.Write(batch.EncodeForPersistance())
	return buf.Bytes()
}

func (w *Top) Batch(qId int, del middleware.Delivery) {
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
		err := handleTop(w, clientId, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	// Persist once the entire delivery is processed
	state := w.Encode(clientId)
	w.persistor.Store(del.Id(), PERSISTOR_FILENAME, state)
}

func (w *Top) Eof(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	responseFieldMaps := make([]map[string]string, 0, w.Con.Amount)
	for _, tup := range w.tops[clientId] {
		responseFieldMaps = append(responseFieldMaps, tup.fieldMap)
	}

	// Check for duplicated deliveries
	if w.persistor.IsDup(del) {
		return
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	body := del.Body
	eof := comms.DecodeEof(body)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	// Persist once the entire delivery is processed
	state := w.Encode(clientId)
	w.persistor.Store(del.Id(), PERSISTOR_FILENAME, state)
}

func (w *Top) Flush(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	w.clean(clientId)
	flush := comms.DecodeFlush(body)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
