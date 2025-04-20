package impl

import (
	"workers"
	"workers/protocol"
	"workers/top/config"

	"github.com/op/go-logging"
)

type Top struct {
	*workers.Worker
	top_lits []map[string]string
}

func New(con *config.TopConfig) (*Top, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Top{base, []map[string]string{}}, nil
}

func (w *Top) Run(con *config.TopConfig, log *logging.Logger) error {
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

			err = handleTop(w, fieldMap, con)
			if err != nil {
				log.Errorf("failed to handle message: %v", err)
				msg.Nack(false, false)
				continue
			}

			// if msg EOF publish top

			msg.Ack(false)
		}
	}

	return nil
}

func handleTop(w *Top, fieldMap map[string]string, con *config.TopConfig) error {

	return nil
}
