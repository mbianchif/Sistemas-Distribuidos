package impl

import (
	"fmt"
	"maps"
	"strings"

	"workers"
	"workers/explode/config"
	"workers/protocol"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
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

	handlers := map[int]func(*Explode, amqp.Delivery, []byte) bool{
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
			exit = handlers[kind](w, del, data)
		}
	}

	return nil
}

func handleBatch(w *Explode, del amqp.Delivery, data []byte) bool {
	batch := protocol.DecodeBatch(data)
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMapSlice, err := handleExplode(fieldMap, w.Con)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			del.Nack(false, false)
			continue
		}

		if responseFieldMapSlice != nil {
			for _, responseFieldMap := range responseFieldMapSlice {
				responseFieldMaps = append(responseFieldMaps, responseFieldMap)
			}
		}
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		body := protocol.NewBatch(responseFieldMaps).Encode(w.Con.Select)
		outQKey := w.Con.OutputQueueKeys[0]
		if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	del.Ack(false)
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

func handleEof(w *Explode, del amqp.Delivery, data []byte) bool {
	body := protocol.DecodeEof(data).Encode()
	outQKey := w.Con.OutputQueueKeys[0]
	if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	del.Ack(false)
	return true
}

func handleError(w *Explode, del amqp.Delivery, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
