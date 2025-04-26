package impl

import (
	"analyzer/workers"
	"analyzer/workers/groupby/config"
	"analyzer/comms"

	"github.com/op/go-logging"
)

const SEP = "<|>"

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
	return w.Worker.Run(w)
}

func (w *Groupby) Batch(data []byte) bool {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatalf("failed to decode batch: %v", err)
	}

	for _, fieldMap := range batch.FieldMaps {
		err := w.Handler.Add(fieldMap, w.Con)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	return false
}

func (w *Groupby) Eof(data []byte) bool {
	responseFieldMaps := w.Handler.Result(w.Con)
	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		if err := w.PublishBatch(batch); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	eof := comms.DecodeEof(data)
	if err := w.PublishEof(eof); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func (w *Groupby) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
