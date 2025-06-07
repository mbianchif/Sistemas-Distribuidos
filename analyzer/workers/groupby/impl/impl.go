package impl

import (
	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/comms/persistance"
	"analyzer/workers"
	"analyzer/workers/groupby/config"

	"github.com/op/go-logging"
)

const SEP = "<|>"
const STATE_DIRNAME = "persistor"

type GroupBy struct {
	*workers.Worker
	con       *config.GroupbyConfig
	handler   GroupByHandler
	persistor persistance.Persistor
}

type GroupByHandler interface {
	add(map[string]string, *config.GroupbyConfig) error
	result(int, *config.GroupbyConfig, persistance.Persistor) []map[string]string
	store(middleware.DelId, *persistance.Persistor) error
}

func New(con *config.GroupbyConfig, log *logging.Logger) (*GroupBy, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	handler := map[string]func(*GroupBy) GroupByHandler{
		"count": NewCount,
		"sum":   NewSum,
		"mean":  NewMean,
	}[con.Aggregator]

	w := &GroupBy{
		Worker:    base,
		con:       con,
		handler:   nil,
		persistor: persistance.New(STATE_DIRNAME, con.InputCopies[0], log),
	}
	w.handler = handler(w)

	return w, nil
}

func (w *GroupBy) Run() error {
	return w.Worker.Run(w)
}

func (w *GroupBy) clean(clientId int) {}

func (w *GroupBy) Batch(qId int, del middleware.Delivery) {
	body := del.Body

	batch, err := comms.DecodeBatch(body)
	if err != nil {
		w.Log.Fatalf("failed to decode batch: %v", err)
	}

	for _, fieldMap := range batch.FieldMaps {
		err := w.handler.add(fieldMap, w.con)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	// Persist once the entire delivery is processed
	w.handler.store(del.Id(), &w.persistor)
}

func (w *GroupBy) Eof(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	responseFieldMaps := w.handler.result(clientId, w.con, w.persistor)

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

func (w *GroupBy) Flush(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	w.clean(clientId)
	flush := comms.DecodeFlush(body)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
