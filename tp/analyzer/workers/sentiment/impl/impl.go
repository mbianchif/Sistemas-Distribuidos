package impl

import (
	"fmt"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/workers"
	"analyzer/workers/sentiment/config"

	"github.com/cdipaolo/sentiment"
	"github.com/op/go-logging"
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
	return w.Worker.Run(w)
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

func (w *Sentiment) Batch(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	batch, err := comms.DecodeBatch(body)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMap, err := handleSentiment(w, fieldMap)
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
		batch := comms.NewBatch(responseFieldMaps)
		if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}
}

func (w *Sentiment) Eof(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	eof := comms.DecodeEof(body)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Sentiment) Flush(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	flush := comms.DecodeFlush(body)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Sentiment) Purge(qId int, del middleware.Delivery) {
	body := del.Body

	purge := comms.DecodePurge(body)
	if err := w.Mailer.PublishPurge(purge); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Sentiment) Close() {
	w.Worker.Close()
}
