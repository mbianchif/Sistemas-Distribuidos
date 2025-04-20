package impl

import (
	"workers"
	"workers/protocol"
	"workers/sink/config"

	"github.com/op/go-logging"
)

type Sink struct {
	*workers.Worker
}

func New(con *config.SinkConfig) (*Sink, error) {
	base, err := workers.New(con.Config)
	if err != nil {
		return nil, err
	}
	return &Sink{base}, nil
}

func (w *Sink) Run(con *config.SinkConfig, log *logging.Logger) error {
	inputQueue := w.InputQueues[0]
	recvChan, err := w.Broker.Consume(inputQueue, "")
	if err != nil {
		return err
	}

	handler := map[string]func(*Sink, map[string]string, *config.SinkConfig) (map[string]string, error){
		"1": handleFirstQuery,
		"2": handleSecondQuery,
		"3": handleThirdQuery,
		"4": handleFourthQuery,
		"5": handleFifthQuery,
	}[con.Query]
	log.Infof("Running with Sink-q%v", con.Query)

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

			responseFieldMap, err := handler(w, fieldMap, con)
			if err != nil {
				log.Errorf("failed to handle message: %v", err)
				msg.Nack(false, false)
				continue
			}

			if responseFieldMap != nil {
				log.Debugf("fieldMap: %v", fieldMap)
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

func handleFirstQuery(w *Sink, msg map[string]string, con *config.SinkConfig) (map[string]string, error) {
	msg["query"] = "1"
	return msg, nil
}

func handleSecondQuery(w *Sink, msg map[string]string, con *config.SinkConfig) (map[string]string, error) {
	msg["query"] = "2"
	return msg, nil
}

func handleThirdQuery(w *Sink, msg map[string]string, con *config.SinkConfig) (map[string]string, error) {
	msg["query"] = "3"
	return msg, nil
}

func handleFourthQuery(w *Sink, msg map[string]string, con *config.SinkConfig) (map[string]string, error) {
	msg["query"] = "4"
	return msg, nil
}

func handleFifthQuery(w *Sink, msg map[string]string, con *config.SinkConfig) (map[string]string, error) {
	msg["query"] = "5"
	return msg, nil
}
