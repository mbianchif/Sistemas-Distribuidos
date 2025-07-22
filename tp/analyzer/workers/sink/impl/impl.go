package impl

import (
	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/workers"
	"analyzer/workers/sink/config"

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

func (w *Sink) Run() error {
	return w.Worker.Run(w)
}

func (w *Sink) Batch(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	batch, err := comms.DecodeBatch(body)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}
	responseFieldMaps := batch.FieldMaps

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		headers := middleware.Table{"query": w.Con.Query}
		if err := w.Mailer.PublishBatch(batch, clientId, headers); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}
}

func (w *Sink) Eof(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	eof := comms.DecodeEof(body)
	headers := middleware.Table{"query": w.Con.Query}
	if err := w.Mailer.PublishEof(eof, clientId, headers); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Sink) Flush(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	body := del.Body

	flush := comms.DecodeFlush(body)
	headers := middleware.Table{"query": w.Con.Query}
	if err := w.Mailer.PublishFlush(flush, clientId, headers); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Sink) Purge(qId int, del middleware.Delivery) {
	body := del.Body

	purge := comms.DecodePurge(body)
	headers := middleware.Table{"query": w.Con.Query}
	if err := w.Mailer.PublishPurge(purge, headers); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Sink) Close() {
	w.Worker.Close()
}
