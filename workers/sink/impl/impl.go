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

func (w *Sink) Run() error {
	return w.Worker.Run(w)
}

func (w *Sink) Batch(data []byte) bool {
	batch, err := protocol.DecodeBatch(data)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}
	responseFieldMaps := batch.FieldMaps

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := protocol.NewBatch(responseFieldMaps)
		if err := w.PublishBatchWithQuery(batch, w.Con.Query); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return false
}

func (w *Sink) Eof(data []byte) bool {
	eof := protocol.DecodeEof(data)
	if err := w.PublishEofWithQuery(eof, w.Con.Query); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
	return true
}

func (w *Sink) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
