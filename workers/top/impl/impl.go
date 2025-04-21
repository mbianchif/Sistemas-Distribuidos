package impl

import (
	"sort"
	"strconv"
	"workers"
	"workers/protocol"
	"workers/top/config"

	"github.com/op/go-logging"
)

type Top struct {
	*workers.Worker
	Con     *config.TopConfig
	top_lit []map[string]string
}

func New(con *config.TopConfig, log *logging.Logger) (*Top, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &Top{base, con, make([]map[string]string, 0)}, nil
}

func (w *Top) Run() error {
	return w.Worker.Run(w)
}

func (w *Top) Batch(data []byte) bool {
	batch, err := protocol.DecodeBatch(data)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}

	for _, fieldMap := range batch.FieldMaps {
		err := handleTop(w, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	return false
}

func (w *Top) Eof(data []byte) bool {
	responseFieldMaps := w.top_lit
	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", w.top_lit)
		body := protocol.NewBatch(w.top_lit).Encode(w.Con.Select)
		if err := w.Broker.Publish("", body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	body := protocol.DecodeEof(data).Encode()
	if err := w.Broker.Publish("", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func (w *Top) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}

func handleTop(w *Top, fieldMap map[string]string) error {
	_, err := strconv.ParseFloat(fieldMap[w.Con.Key], 64)
	if err != nil {
		return err
	}
	w.top_lit = append(w.top_lit, fieldMap)

	sort.Slice(w.top_lit, func(i, j int) bool {
		vi, _ := strconv.ParseFloat(w.top_lit[i][w.Con.Key], 64)
		vj, _ := strconv.ParseFloat(w.top_lit[j][w.Con.Key], 64)
		return vi > vj
	})

	if len(w.top_lit) > w.Con.Amount {
		w.top_lit = w.top_lit[:w.Con.Amount]
	}
	return nil
}
