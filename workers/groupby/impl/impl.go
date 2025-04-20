package impl

import (
	"fmt"
	"workers"
	"workers/groupby/config"
	"workers/protocol"

	"github.com/op/go-logging"
)

type Groupby struct {
	*workers.Worker
}

type GroupbyHandler interface {
	Add(map[string]string, *config.GroupbyConfig) error
	Result(*config.GroupbyConfig) []map[string]string
}

func New(con *config.GroupbyConfig) (*Groupby, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Groupby{base}, nil
}

func (w *Groupby) Run(con *config.GroupbyConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handler := map[string]func(*Groupby) GroupbyHandler{
		"count": NewCount,
		"sum":   NewSum,
		"mean":  NewMean,
	}[con.Aggregator](w)

	log.Infof("Running with aggregator: %v", con.Aggregator)
	exit := false
	for !exit {
		select {
		case <-w.SigChan:
			exit = true

		case msg := <-recvChan:
			fieldMap, err := protocol.Decode(msg.Body)
			if err != nil {
				log.Errorf("failed to decode message: %v", err)
				msg.Nack(false, false)
				continue
			}

			err = handler.Add(fieldMap, con)
			if err != nil {
				log.Errorf("failed to handle message: %v", err)
				msg.Nack(false, false)
				continue
			}

			msg.Ack(false)
		}
	}

	return nil
}
