package impl

import (
	"fmt"
	"maps"
	"strings"

	"workers"
	"workers/explode/config"
	"workers/protocol"

	"github.com/op/go-logging"
)

type Explode struct {
	*workers.Worker
	Con *config.ExplodeConfig
}

func New(con *config.ExplodeConfig, log *logging.Logger) (*Explode, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &Explode{base, con}, nil
}

func (w *Explode) Run() error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*Explode, []byte) bool{
		protocol.BATCH: handleBatch,
		protocol.EOF:   handleEof,
		protocol.ERROR: handleError,
	}

	w.Log.Infof("Running")
	exit := false
	for !exit {
		select {
		case <-w.SigChan:
			exit = true

		case del := <-recvChan:
			kind, data := protocol.ReadDelivery(del)
			exit = handlers[kind](w, data)
			del.Ack(false)
		}
	}

	return nil
}

func handleBatch(w *Explode, data []byte) bool {
	batch := protocol.DecodeBatch(data)
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMapSlice, err := handleExplode(fieldMap, w.Con)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}

		responseFieldMaps = append(responseFieldMaps, responseFieldMapSlice...)
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		body := protocol.NewBatch(responseFieldMaps).Encode(w.Con.Select)
		if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return false
}

func handleExplode(fieldMap map[string]string, con *config.ExplodeConfig) ([]map[string]string, error) {
	values, ok := fieldMap[con.Key]
	if !ok {
		return nil, fmt.Errorf("%v is not a field in the message", con.Key)
	}

	fieldMaps := make([]map[string]string, 0)
	for value := range strings.SplitSeq(values, ",") {
		expCopy := maps.Clone(fieldMap)
		expCopy[con.Rename] = value
		fieldMaps = append(fieldMaps, expCopy)
	}

	return fieldMaps, nil
}

func handleEof(w *Explode, data []byte) bool {
	body := protocol.DecodeEof(data).Encode()
	if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func handleError(w *Explode, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
