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
	seq          []int
}

func NewRobin(broker *Broker, fmt string, outputCopies int) *SenderRobin {
	return &SenderRobin{
		broker:       broker,
		fmt:          fmt,
		outputCopies: outputCopies,
		seq:          make([]int, outputCopies),
	}
}

func (s *SenderRobin) Batch(batch comms.Batch, filterCols map[string]struct{}, headers amqp.Table) error {
	body := batch.Encode(filterCols)
	return s.Direct(body, headers)
}

func (s *SenderRobin) Eof(eof comms.Eof, headers amqp.Table) error {
	body := eof.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderRobin) nextKeySeq() (string, int) {
	key := fmt.Sprintf(s.fmt, s.i)
	seq := s.seq[s.i]

	s.seq[s.i] += 1
	s.i = (s.i + 1) % s.outputCopies
	return key, seq
}

func (s *SenderRobin) Direct(body []byte, headers amqp.Table) error {
	key, seq := s.nextKeySeq()
	headers["seq"] = seq
	return s.broker.Publish(key, body, headers)
}

func (s *SenderRobin) Broadcast(body []byte, headers amqp.Table) error {
	for range s.outputCopies {
		if err := s.Direct(body, headers); err != nil {
			return err
		}
	}
	return nil
}
