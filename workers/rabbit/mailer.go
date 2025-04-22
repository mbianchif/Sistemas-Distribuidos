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

	}
	return s.broker.PublishWithHeaders(key, body, headers)
}

type senderShard struct {
	key string
}
