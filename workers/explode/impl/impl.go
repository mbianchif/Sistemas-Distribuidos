package impl

import (
	"fmt"
	"maps"
	"strings"

	"workers"
	"workers/explode/config"
	"workers/protocol"

	"github.com/op/go-logging"
)

type Explode struct {
	*workers.Worker
}

func New(con *config.ExplodeConfig) (*Explode, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Explode{base}, nil
}

func (w *Explode) Run(con *config.ExplodeConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	log.Infof("Running")
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

			responseFieldMapSlice, err := handleExplode(w, fieldMap, con)
			if err != nil {
				log.Errorf("failed to handle message: %v", err)
				msg.Nack(false, false)
				continue
			}

			for _, responseFieldMap := range responseFieldMapSlice {
				log.Debugf("fieldMap: %v", responseFieldMap)
				body := protocol.Encode(responseFieldMap, con.Select)
				outQKey := con.OutputQueueKeys[0]
				if err := w.Broker.Publish(con.OutputExchangeName, outQKey, body); err != nil {
					log.Errorf("failed to publish message: %v", err)
				}
			}

			msg.Ack(false)
		}
	}

	return nil
}

func handleExplode(w *Explode, fieldMap map[string]string, con *config.ExplodeConfig) ([]map[string]string, error) {
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
