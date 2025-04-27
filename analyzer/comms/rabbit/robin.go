package rabbit

import (
	"fmt"

	"analyzer/comms"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SenderRobin struct {
	broker       *Broker
	fmt          string
	i            int
	outputCopies int
}

func NewRobin(broker *Broker, fmt string, outputCopies int) *SenderRobin {
	return &SenderRobin{
		broker:       broker,
		fmt:          fmt,
		outputCopies: outputCopies,
	}
}

func (s *SenderRobin) nextKey() string {
	key := fmt.Sprintf(s.fmt, s.i)
	s.i = (s.i + 1) % s.outputCopies
	return key
}

func (s *SenderRobin) Batch(batch comms.Batch, filterCols map[string]struct{}, headers amqp.Table) error {
	body := batch.Encode(filterCols)
	return s.Direct(body, headers)
}

func (s *SenderRobin) Eof(eof comms.Eof, headers amqp.Table) error {
	body := eof.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderRobin) Direct(body []byte, headers amqp.Table) error {
	key := s.nextKey()
	return s.broker.Publish(key, body, headers)
}

func (s *SenderRobin) Broadcast(body []byte, headers amqp.Table) error {
	for i := range s.outputCopies {
		key := fmt.Sprintf(s.fmt, i)
		if err := s.broker.Publish(key, body, headers); err != nil {
			return err
		}
	}
	return nil
}
