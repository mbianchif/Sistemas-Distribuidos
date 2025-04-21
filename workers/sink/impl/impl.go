package impl

import (
	"workers"
	"workers/protocol"
	"workers/sink/config"

	"github.com/op/go-logging"
)

type Sink struct {
	*workers.Worker
	Con *config.SinkConfig
}

func New(con *config.SinkConfig, log *logging.Logger) (*Sink, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &Sink{base, con}, nil
}

func (w *Sink) Run(con *config.SinkConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*Sink, []byte) bool{
		protocol.BATCH: handleBatch,
		protocol.EOF:   handleEof,
		protocol.ERROR: handleError,
	}

	log.Infof("Running with Sink-q%v", con.Query)
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

func handleBatch(w *Sink, data []byte) bool {
	batch := protocol.DecodeBatch(data)
	responseFieldMaps := batch.FieldMaps

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		body := protocol.NewBatch(responseFieldMaps).EncodeWithQuery(w.Con.Select, w.Con.Query)
		if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return false
}

func handleEof(w *Sink, data []byte) bool {
	body := protocol.DecodeEof(data).EncodeWithQuery(w.Con.Query)
	if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func handleError(w *Sink, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
