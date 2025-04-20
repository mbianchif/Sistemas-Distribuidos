package impl

import (
	"workers"
	"workers/protocol"
	"workers/sink/config"

	"github.com/op/go-logging"
)

type Sink struct {
	*workers.Worker
	Con     *config.SinkConfig
	Handler func(*Sink, map[string]string) (map[string]string, error)
}

func New(con *config.SinkConfig, log *logging.Logger) (*Sink, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	handler := map[string]func(*Sink, map[string]string) (map[string]string, error){
		"1": handleFirstQuery,
		"2": handleSecondQuery,
		"3": handleThirdQuery,
		"4": handleFourthQuery,
		"5": handleFifthQuery,
	}[con.Query]
	return &Sink{base, con, handler}, nil
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
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMap, err := w.Handler(w, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}

		if responseFieldMap != nil {
			responseFieldMaps = append(responseFieldMaps, responseFieldMap)
		}
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

func handleEof(w *Sink, data []byte) bool {
	body := protocol.DecodeEof(data).Encode()
	if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func handleError(w *Sink, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}

func handleFirstQuery(w *Sink, msg map[string]string) (map[string]string, error) {
	msg["query"] = "1"
	return msg, nil
}

func handleSecondQuery(w *Sink, msg map[string]string) (map[string]string, error) {
	msg["query"] = "2"
	return msg, nil
}

func handleThirdQuery(w *Sink, msg map[string]string) (map[string]string, error) {
	msg["query"] = "3"
	return msg, nil
}

func handleFourthQuery(w *Sink, msg map[string]string) (map[string]string, error) {
	msg["query"] = "4"
	return msg, nil
}

func handleFifthQuery(w *Sink, msg map[string]string) (map[string]string, error) {
	msg["query"] = "5"
	return msg, nil
}
