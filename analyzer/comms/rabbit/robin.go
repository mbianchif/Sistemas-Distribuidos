package rabbit

import (
	"fmt"

	"analyzer/comms"

	"github.com/op/go-logging"
)

type SenderRobin struct {
	broker       *Broker
	fmt          string
	i            int
	outputCopies int
	log          *logging.Logger
}

func NewRobin(broker *Broker, fmt string, outputCopies int, log *logging.Logger) *SenderRobin {
	return &SenderRobin{
		broker:       broker,
		fmt:          fmt,
		outputCopies: outputCopies,
		log:          log,
	}
}

func (s *SenderRobin) nextKey() string {
	key := fmt.Sprintf(s.fmt, s.i)
	s.i = (s.i + 1) % s.outputCopies
	return key
}

func (s *SenderRobin) Batch(batch comms.Batch, filterCols map[string]struct{}) error {
	key := s.nextKey()
	body := batch.Encode(filterCols)
	return s.broker.Publish(key, body)
}

func (s *SenderRobin) BatchWithQuery(batch comms.Batch, filterCols map[string]struct{}, query int) error {
	key := s.nextKey()
	body := batch.EncodeWithQuery(filterCols, query)
	return s.broker.Publish(key, body)
}

func (s *SenderRobin) Eof(eof comms.Eof) error {
	body := eof.Encode()
	return s.broadcast(body)
}

func (s *SenderRobin) EofWithQuery(eof comms.Eof, query int) error {
	body := eof.EncodeWithQuery(query)
	return s.broadcast(body)
}

func (s *SenderRobin) Error(erro comms.Error) error {
	body := erro.Encode()
	return s.broadcast(body)
}

func (s *SenderRobin) broadcast(body []byte) error {
	for i := range s.outputCopies {
		key := fmt.Sprintf(s.fmt, i)
		if err := s.broker.Publish(key, body); err != nil {
			return err
		}
	}
	return nil
}
