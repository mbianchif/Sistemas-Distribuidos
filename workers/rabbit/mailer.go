package rabbit

import (
	"fmt"
	"workers/protocol"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Mailer struct {
	senders []sender
}

func (m *Mailer) PublishBatch(batch protocol.Batch, filterCols map[string]struct{}) error {
	for _, sender := range m.senders {
		if err := sender.Batch(batch, filterCols); err != nil {
			return err
		}
	}

	return nil
}

type sender interface {
	Batch(protocol.Batch, map[string]struct{}) error
	Eof(protocol.Eof) error
}

type senderDirect struct {
	broker *Broker
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

	for range s.n {
		key := s.nextKey()
		if err := s.broker.PublishWithHeaders(key, body, headers); err != nil {
			// TODO: log
			continue
		}
	}

	return nil
}

type senderShard struct {
	key string
}
