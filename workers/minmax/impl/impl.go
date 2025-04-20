package impl

import (
	"strconv"
	"workers"
	"workers/minmax/config"
	"workers/protocol"

	"github.com/op/go-logging"
)

type MinMax struct {
	*workers.Worker
	Con *config.MinMaxConfig
	min map[string]string
	max map[string]string
}

func New(con *config.MinMaxConfig, log *logging.Logger) (*MinMax, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &MinMax{base, con, make(map[string]string), make(map[string]string)}, nil
}

func (w *MinMax) Run(con *config.MinMaxConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*MinMax, []byte) bool{
		protocol.BATCH: handleBatch,
		protocol.EOF:   handleEof,
		protocol.ERROR: handleError,
	}

	log.Infof("Running")
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

func handleBatch(w *MinMax, data []byte) bool {
	batch := protocol.DecodeBatch(data)

	for _, fieldMap := range batch.FieldMaps {
		err := handleMinMax(w, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	return false
}

func handleEof(w *MinMax, data []byte) bool {
	responseFieldMaps := make([]map[string]string, 0, 2)
	responseFieldMaps = append(responseFieldMaps, w.min)
	responseFieldMaps = append(responseFieldMaps, w.max)
	w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
	body := protocol.NewBatch(responseFieldMaps).Encode(w.Con.Select)
	if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	body = protocol.DecodeEof(data).Encode()
	if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func handleError(w *MinMax, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}

func handleMinMax(w *MinMax, fieldMap map[string]string) error {
	if w.max[w.Con.Key] == "" {
		w.max = fieldMap
	}
	if w.min[w.Con.Key] == "" {
		w.min = fieldMap
	}
	newValue, err := strconv.ParseFloat(fieldMap[w.Con.Key], 64)
	if err != nil {
		return err
	}
	maxValue, err := strconv.ParseFloat(w.max[w.Con.Key], 64)
	if err != nil {
		return err
	}
	minValue, err := strconv.ParseFloat(w.min[w.Con.Key], 64)
	if err != nil {
		return err
	}
	if newValue > maxValue {
		w.max = fieldMap
	}
	if newValue < minValue {
		w.min = fieldMap
	}
	return nil
}
