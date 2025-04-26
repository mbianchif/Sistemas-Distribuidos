package impl

import (
	"fmt"
	"maps"
	"strings"

	"analyzer/workers"
	"analyzer/workers/explode/config"
	"analyzer/comms"

	"github.com/op/go-logging"
)

type Explode struct {
	*workers.Worker
	Con *config.ExplodeConfig
}

func New(con *config.ExplodeConfig, log *logging.Logger) (*Explode, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &Explode{base, con}, nil
}

func (w *Explode) Run() error {
	return w.Worker.Run(w)
}

func (w *Explode) Batch(data []byte) bool {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatal("failed to decode line: %v", err)
	}
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMapSlice, err := handleExplode(fieldMap, w.Con)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}

		responseFieldMaps = append(responseFieldMaps, responseFieldMapSlice...)
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		body := comms.NewBatch(responseFieldMaps)
		if err := w.PublishBatch(body); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	return false
}

func handleExplode(fieldMap map[string]string, con *config.ExplodeConfig) ([]map[string]string, error) {
	values, ok := fieldMap[con.Key]
	if !ok {
		return nil, fmt.Errorf("%v is not a field in the message", con.Key)
	}

	fieldMaps := make([]map[string]string, 0)
	for value := range strings.SplitSeq(values, ",") {
		expCopy := maps.Clone(fieldMap)
		expCopy[con.Rename] = value
		fieldMaps = append(fieldMaps, expCopy)
	}

	return fieldMaps, nil
}

func (w *Explode) Eof(data []byte) bool {
	eof := comms.DecodeEof(data)
	if err := w.PublishEof(eof); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func (w *Explode) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
