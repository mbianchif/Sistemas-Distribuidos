package impl

import (
	"fmt"
	"workers"
	"workers/protocol"
	"workers/sentiment/config"

	"github.com/cdipaolo/sentiment"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Sentiment struct {
	*workers.Worker
	Con   *config.SentimentConfig
	Model sentiment.Models
}

func New(con *config.SentimentConfig, log *logging.Logger) (*Sentiment, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	model, err := sentiment.Restore()
	if err != nil {
		return nil, err
	}

	return &Sentiment{base, con, model}, nil
}

func (w *Sentiment) Run() error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*Sentiment, amqp.Delivery, []byte) bool{
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

func handleBatch(w *Sentiment, del amqp.Delivery, data []byte) bool {
	batch := protocol.DecodeBatch(data)
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMap, err := handleSentiment(w, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			del.Nack(false, false)
			continue
		}

		if responseFieldMap != nil {
			responseFieldMaps = append(responseFieldMaps, responseFieldMap)
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

func handleSentiment(w *Sentiment, fieldMap map[string]string) (map[string]string, error) {
	overview, ok := fieldMap["overview"]
	if !ok {
		return nil, fmt.Errorf("no overview in field map")
	}

	if len(overview) == 0 {
		return nil, nil
	}

	analysis := w.Model.SentimentAnalysis(overview, sentiment.English)
	var result string
	if analysis.Score == 0 {
		result = "negative"
	} else {
		result = "positive"
	}

	fieldMap["sentiment"] = result
	return fieldMap, nil
}

func handleEof(w *Sentiment, del amqp.Delivery, data []byte) bool {
	body := protocol.DecodeEof(data).Encode()
	outQKey := w.Con.OutputQueueKeys[0]
	if err := w.Broker.Publish(w.Con.OutputExchangeName, outQKey, body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	del.Ack(false)
	return true
}

func handleError(w *Sentiment, del amqp.Delivery, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
