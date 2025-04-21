package impl

import (
	"workers"
	"workers/groupby/config"
	"workers/protocol"

	"github.com/op/go-logging"
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
	return w.Worker.Run(w)
}

func (w *Groupby) Batch(data []byte) bool {
	batch := protocol.DecodeBatch(data)

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
	fieldMaps := w.Handler.Result(w.Con)
	body := protocol.NewBatch(fieldMaps).Encode(w.Con.Select)
	if err := w.Broker.Publish("", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	body = protocol.DecodeEof(data).Encode()
	if err := w.Broker.Publish("", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func (w *Groupby) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
