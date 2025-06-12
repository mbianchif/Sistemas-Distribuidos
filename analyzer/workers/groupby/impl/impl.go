package impl

import (
	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/comms/persistance"
	"analyzer/workers"
	"analyzer/workers/groupby/config"

	"github.com/op/go-logging"
)

const STATE_DIRNAME = "persistor"

type GroupBy struct {
	*workers.Worker
	con       *config.GroupByConfig
	handler   GroupByHandler
	persistor persistance.Persistor
}

type GroupByHandler interface {
	add(map[string][]map[string]string, config.GroupByConfig) error
	result(int, config.GroupByConfig, persistance.Persistor) ([]map[string]string, error)
	store(middleware.DelId, *persistance.Persistor) error
}

func New(con *config.GroupByConfig, log *logging.Logger) (*GroupBy, error) {
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

func (w *GroupBy) Batch(qId int, del middleware.Delivery) {
	body := del.Body

	batch, err := comms.DecodeBatch(body)
	if err != nil {
		w.Log.Fatalf("failed to decode batch: %v", err)
	}

	shardKeys := w.con.GroupKeys
	shards, err := comms.Shard(batch.FieldMaps, shardKeys, func(s string) string { return s })
	if err != nil {
		w.Log.Errorf("failed to shard batch: %v", err)
		return
	}

	if err := w.handler.add(shards, *w.con); err != nil {
		w.Log.Errorf("failed to handle message: %v", err)
		return
	}

	// Persist once the entire delivery is processed
	w.handler.store(del.Id(), &w.persistor)
}

func (w *GroupBy) Eof(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	responseFieldMaps, _ := w.handler.result(clientId, *w.con, w.persistor)

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

func (w *GroupBy) flush(clientId int) {
	if err := w.persistor.Flush(clientId); err != nil {
		w.Log.Errorf("failed to flush inner state for client %d: %v", clientId, err)
	}
}

func (w *GroupBy) Flush(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	w.flush(clientId)
	flush := comms.DecodeFlush(body)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *GroupBy) purge() {
	if err := w.persistor.Purge(); err != nil {
		w.Log.Errorf("failed to purge inner state: %v", err)
	}

}

func (w *GroupBy) Purge(qId int, del middleware.Delivery) {
	body := del.Body

	w.purge()
	purge := comms.DecodePurge(body)
	if err := w.Mailer.PublishPurge(purge); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
