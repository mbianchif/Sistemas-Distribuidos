package rabbit

import (
	"fmt"

	"workers/protocol"
	"workers/rabbit"

	amqp "github.com/rabbitmq/amqp091-go"
)

type senderDirect struct {
	broker *rabbit.Broker
	fmt    string
	i      int
	n      int
}

func (s senderDirect) nextKey() string {
	key := fmt.Sprintf(s.fmt, s.i)
	s.i = (s.i + 1) % s.n
	return key
}

func (s senderDirect) Batch(batch protocol.Batch, filterCols map[string]struct{}) error {
	key := s.nextKey()
	body := batch.Encode(filterCols)
	headers := amqp.Table{
		"type": protocol.BATCH,
	}
	return s.broker.PublishWithHeaders(key, body, headers)
}

func (s senderDirect) Eof(eof protocol.Eof) error {
	body := eof.Encode()
	headers := amqp.Table{
		"type": protocol.EOF,
	}
	return s.broadcast(body, headers)
}

func (s senderDirect) Error(erro protocol.Error) error {
	body := erro.Encode()
	headers := amqp.Table{
		"type": protocol.ERROR,
	}
	return s.broadcast(body, headers)
}

func (s senderDirect) broadcast(body []byte, headers amqp.Table) error {
	for i := range s.n {
		key := fmt.Sprintf(s.fmt, i)
		if err := s.broker.PublishWithHeaders(key, body, headers); err != nil {
			// TODO: log
			continue
		}
	}

	return nil
}
