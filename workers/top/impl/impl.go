package impl

import (
	"sort"
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

func (w *Top) Run(con *config.TopConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handlers := map[int]func(*Top, []byte) bool{
		protocol.BATCH: handleBatch,
		protocol.EOF:   handleEof,
		protocol.ERROR: handleError,
	}

	log.Infof("Running")
	exit := false
	for !exit {
		select {
		case <-w.SigChan:
			exit = true

		case del := <-recvChan:
			kind, data := protocol.ReadDelivery(del)
			exit = handlers[kind](w, data)
			del.Ack(false)
		}
	}

	return nil
}

func handleBatch(w *Top, data []byte) bool {
	batch := protocol.DecodeBatch(data)

	for _, fieldMap := range batch.FieldMaps {
		err := handleTop(w, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}

	return false
}

func handleEof(w *Top, data []byte) bool {

	w.Log.Debugf("fieldMaps: %v", w.top_lit)
	body := protocol.NewBatch(w.top_lit).Encode(w.Con.Select)
	if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	body = protocol.DecodeEof(data).Encode()
	if err := w.Broker.Publish(w.Con.OutputExchangeName, "", body); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	return true
}

func handleError(w *Top, data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}

func handleTop(w *Top, fieldMap map[string]string) error {
	w.top_lit = append(w.top_lit, fieldMap)

	sort.Slice(w.top_lit, func(i, j int) bool {
		return w.top_lit[i][w.Con.Key] > w.top_lit[j][w.Con.Key]
	})

	if len(w.top_lit) > w.Con.Amount {
		w.top_lit = w.top_lit[:w.Con.Amount]
	}
	return nil
}
