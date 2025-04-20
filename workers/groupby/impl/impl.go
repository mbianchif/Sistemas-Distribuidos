package impl

import (
	"workers"
	"workers/groupby/config"
	"workers/protocol"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Groupby struct {
	*workers.Worker
	Con     *config.GroupbyConfig
	Handler GroupbyHandler
}

type GroupbyHandler interface {
	Add(map[string]string, *config.GroupbyConfig) error
	Result(*config.GroupbyConfig) []map[string]string
}

func New(con *config.GroupbyConfig, log *logging.Logger) (*Groupby, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	handler := map[string]func(*Groupby) GroupbyHandler{
		"count": NewCount,
		"sum":   NewSum,
		"mean":  NewMean,
	}[con.Aggregator]

	w := &Groupby{base, con, nil}
	w.Handler = handler(w)
	return w, nil
}

func (w *Groupby) Run() error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*Groupby, amqp.Delivery, []byte) bool{
		protocol.BATCH: handleBatch,
		protocol.EOF:   handleEof,
		protocol.ERROR: handleError,
	}

	w.Log.Infof("Running with aggregator: %v", w.Con.Aggregator)
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

func handleBatch(w *Groupby, del amqp.Delivery, data []byte) bool {
	batch := protocol.DecodeBatch(data)

	for _, fieldMap := range batch.FieldMaps {
		err := w.Handler.Add(fieldMap, w.Con)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	del.Ack(false)
	return false
}

func handleEof(w *Groupby, del amqp.Delivery, data []byte) bool {
	fieldMaps := w.Handler.Result(w.Con)
	body := protocol.NewBatch(fieldMaps).Encode(w.Con.Select)
	outQKey := w.Con.OutputQueueKeys[0]
	if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	body = protocol.DecodeEof(data).Encode()
	if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	del.Ack(false)
	return true
}

func handleError(w *Groupby, del amqp.Delivery, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
