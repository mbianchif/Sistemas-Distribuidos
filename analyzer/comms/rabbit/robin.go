package rabbit

import (
	"fmt"

	"analyzer/comms"
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

func (s *SenderRobin) Batch(batch comms.Batch, filterCols map[string]struct{}) error {
	body := batch.Encode(filterCols)
	return s.Direct(body)
}

func (s *SenderRobin) BatchWithQuery(batch comms.Batch, filterCols map[string]struct{}, query int) error {
	body := batch.EncodeWithQuery(filterCols, query)
	return s.Direct(body)
}

func (s *SenderRobin) Eof(eof comms.Eof) error {
	body := eof.Encode()
	return s.Broadcast(body)
}

func (s *SenderRobin) EofWithQuery(eof comms.Eof, query int) error {
	body := eof.EncodeWithQuery(query)
	return s.Broadcast(body)
}

func (s *SenderRobin) Error(erro comms.Error) error {
	body := erro.Encode()
	return s.Broadcast(body)
}

func (s *SenderRobin) Direct(body []byte) error {
	key := s.nextKey()
	return s.broker.Publish(key, body)
}

func (s *SenderRobin) Broadcast(body []byte) error {
	for i := range s.outputCopies {
		key := fmt.Sprintf(s.fmt, i)
		if err := s.broker.Publish(key, body); err != nil {
			return err
		}
	}
	return nil
}
