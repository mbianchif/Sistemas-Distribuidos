package rabbit

import (
	"fmt"

	"workers/protocol"

	"github.com/op/go-logging"
)

type SenderShard struct {
	broker       *Broker
	fmt          string
	key          string
	outputCopies int
	log          *logging.Logger
}

func NewShard(broker *Broker, fmt string, key string, outputCopies int, log *logging.Logger) *SenderShard {
	return &SenderShard{
		broker: broker,
		fmt: fmt,
		key: key,
		outputCopies: outputCopies,
		log: log,
	}
}

func keyHash(field string, mod int) int {
	acc := 0
	for _, ch := range field {
		acc += int(ch)
	}
	return acc % mod
}

func (s *SenderShard) shard(fieldMaps []map[string]string) (map[int][]map[string]string, error) {
	shards := make(map[int][]map[string]string, s.outputCopies)

	for _, fieldMap := range fieldMaps {
		key, ok := fieldMap[s.key]
		if !ok {
			return nil, fmt.Errorf("left key %v was not found in field map", s.key)
		}

		shardKey := keyHash(key, s.outputCopies)
		shards[shardKey] = append(shards[shardKey], fieldMap)
	}

	return shards, nil
}

func (s *SenderShard) Batch(batch protocol.Batch, filterCols map[string]struct{}) error {
	shards, err := s.shard(batch.FieldMaps)
	if err != nil {
		return err
	}

	for i, shard := range shards {
		key := fmt.Sprintf(s.fmt, i)
		body := protocol.NewBatch(shard).Encode(filterCols)
		if err := s.broker.Publish(key, body); err != nil {
			s.log.Errorf("error while publishing sharded message to %v", i)
			continue
		}
	}

	return nil
}

func (s *SenderShard) BatchWithQuery(batch protocol.Batch, filterCols map[string]struct{}, query int) error {
	shards, err := s.shard(batch.FieldMaps)
	if err != nil {
		return err
	}

	for i, shard := range shards {
		key := fmt.Sprintf(s.fmt, i)
		body := protocol.NewBatch(shard).EncodeWithQuery(filterCols, query)
		if err := s.broker.Publish(key, body); err != nil {
			return err
		}
	}

	return nil
}

func (s *SenderShard) Eof(eof protocol.Eof) error {
	body := eof.Encode()
	return s.broadcast(body)
}

func (s *SenderShard) EofWithQuery(eof protocol.Eof, query int) error {
	body := eof.EncodeWithQuery(query)
	return s.broadcast(body)
}

func (s *SenderShard) Error(erro protocol.Error) error {
	body := erro.Encode()
	return s.broadcast(body)
}

func (s *SenderShard) broadcast(body []byte) error {
	for i := range s.outputCopies {
		key := fmt.Sprintf(s.fmt, i)
		if err := s.broker.Publish(key, body); err != nil {
			return err
		}
	}
	return nil
}
