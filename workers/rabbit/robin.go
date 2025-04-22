package rabbit

import (
	"fmt"

	"workers/protocol"

	"github.com/op/go-logging"
)

type SenderRobin struct {
	broker *Broker
	fmt    string
	i      int
	n      int
	log    *logging.Logger
}

func NewRobin(broker *Broker, fmt string, n int, log *logging.Logger) *SenderRobin {
	return &SenderRobin{broker, fmt, 0, n, log}
}

func (s *SenderRobin) nextKey() string {
	key := fmt.Sprintf(s.fmt, s.i)
	s.i = (s.i + 1) % s.n
	return key
}

func (s *SenderRobin) Batch(batch protocol.Batch, filterCols map[string]struct{}) error {
	key := s.nextKey()
	body := batch.Encode(filterCols)
	return s.broker.Publish(key, body)
}

func (s *SenderRobin) BatchWithQuery(batch protocol.Batch, filterCols map[string]struct{}, query int) error {
	key := s.nextKey()
	body := batch.EncodeWithQuery(filterCols, query)
	return s.broker.Publish(key, body)
}

func (s *SenderRobin) Eof(eof protocol.Eof) error {
	body := eof.Encode()
	return s.broadcast(body)
}

func (s *SenderRobin) Error(erro protocol.Error) error {
	body := erro.Encode()
	return s.broadcast(body)
}

func (s *SenderRobin) broadcast(body []byte) error {
	for i := range s.n {
		key := fmt.Sprintf(s.fmt, i)
		if err := s.broker.Publish(key, body); err != nil {
			return err
		}
	}
	return nil
}
