package impl

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/comms/persistance"
	"analyzer/workers"
	"analyzer/workers/top/config"

	"github.com/op/go-logging"
)

const STATE_DIRNAME = "persistor"
const STATE_FILENAME = "state"

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

func (w *Top) tryRecover() error {
	persistedFiles, err := w.persistor.Recover()
	if err != nil {
		return err
	}

	for pf := range persistedFiles {
		clientId := pf.ClientId
		state := pf.State

		if err := w.decode(clientId, state); err != nil {
			return err
		}
	}

	return nil
}

func New(con *config.TopConfig, log *logging.Logger) (*Top, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	w := Top{
		Worker:    base,
		Con:       con,
		persistor: persistance.New(STATE_DIRNAME, con.InputCopies[0], log),
		tops:      make(map[int][]tuple),
	}

	if err := w.tryRecover(); err != nil {
		return nil, err
	}

	return &w, nil
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

func (w *Top) decode(clientId int, state []byte) error {
	reader := bufio.NewReader(bytes.NewReader(state))
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			break
		}

		line = bytes.TrimSpace(line)
		fieldMap, err := comms.DecodeLine(line)
		if err != nil {
			return fmt.Errorf("failed to decode line: %v", err)
		}

		handleTop(w, clientId, fieldMap)
	}

	return nil
}

func (w *Top) Batch(qId int, del middleware.Delivery) {
	id := del.Id()
	body := del.Body
	clientId := id.ClientId

	batch, err := comms.DecodeBatch(body)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}

	// Check for duplicated deliveries
	header, err := w.persistor.LoadHeader(clientId, STATE_FILENAME)
	if err == nil && header.IsDup(del.Id()) {
		return
	}

	for _, fieldMap := range batch.FieldMaps {
		err := handleTop(w, clientId, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	// Persist once the entire delivery is processed
	state := w.Encode(clientId)
	w.persistor.Store(id, STATE_FILENAME, state, header)
}

func (w *Top) Eof(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	responseFieldMaps := make([]map[string]string, 0, w.Con.Amount)
	for _, tup := range w.tops[clientId] {
		responseFieldMaps = append(responseFieldMaps, tup.fieldMap)
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
