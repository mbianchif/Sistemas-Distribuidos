package impl

import (
	"fmt"
	"workers"
	"workers/protocol"
	"workers/sentiment/config"

	"github.com/cdipaolo/sentiment"
	"github.com/op/go-logging"
)

type Sentiment struct {
	*workers.Worker
	Model sentiment.Models
}

func New(con *config.SentimentConfig) (*Sentiment, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}

	model, err := sentiment.Restore()
	if err != nil {
		return nil, err
	}

	return &Sentiment{base, model}, nil
}

func (w *Sentiment) Run(con *config.SentimentConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	log.Infof("Running")
	for msg := range recvChan {
		fieldMap, err := protocol.Decode(msg.Body)
		if err != nil {
			log.Errorf("failed to decode message: %v", err)
			msg.Nack(false, false)
			continue
		}

		responseFieldMap, err := handleSentiment(w, fieldMap)
		if err != nil {
			log.Errorf("failed to handle message: %v", err)
			msg.Nack(false, false)
			continue
		}

		if responseFieldMap != nil {
			log.Debugf("fieldMap: %v", responseFieldMap)
			body := protocol.Encode(responseFieldMap, con.Select)
			outQKey := con.OutputQueueKeys[0]
			if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
				log.Errorf("failed to publish message: %v", err)
			}
		}

		msg.Ack(false)
	}

	log.Info("recv channel was closed")
	return nil
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
