package impl

import (
	"fmt"
	"maps"
	"strings"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/workers"
	"analyzer/workers/explode/config"

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

func (w *Explode) Batch(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	data := del.Body
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
		if err := w.Mailer.PublishBatch(body, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

}

func (w *Explode) Eof(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	data := del.Body
	eof := comms.DecodeEof(data)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

}

func (w *Explode) Flush(qId int, del middleware.Delivery) {
	clientId := del.Headers.ClientId
	data := del.Body
	flush := comms.DecodeFlush(data)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Explode) Purge(qId int, del middleware.Delivery) {
	body := del.Body

	purge := comms.DecodePurge(body)
	if err := w.Mailer.PublishPurge(purge); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
