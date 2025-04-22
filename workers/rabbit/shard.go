package rabbit

import (
	"fmt"

	"workers/protocol"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SenderShard struct {
	broker *Broker
	fmt    string
	key    string
	n      int
}

func NewShard(broker *Broker, fmt string, key string, n int) *SenderShard {
	return &SenderShard{broker, fmt, key, n}
}

func keyHash(field string, mod int) int {
	acc := 0
	for _, ch := range field {
		acc += int(ch)
	}
	return acc % mod
}

func (s SenderShard) shard(fieldMaps []map[string]string) (map[int][]map[string]string, error) {
	shards := make(map[int][]map[string]string, s.n)

	for _, fieldMap := range fieldMaps {
		key, ok := fieldMap[s.key]
		if !ok {
			return nil, fmt.Errorf("left key %v was not found in field map", s.key)
		}

		shardKey := keyHash(key, s.n)
		shards[shardKey] = append(shards[shardKey], fieldMap)
	}

	return shards, nil
}

func (s SenderShard) Batch(batch protocol.Batch, filterCols map[string]struct{}) error {
	shards, err := s.shard(batch.FieldMaps)
	if err != nil {
		return err
	}
	headers := amqp.Table{
		"type": protocol.BATCH,
	}

	for i, shard := range shards {
		key := fmt.Sprintf(s.fmt, i)
		body := protocol.NewBatch(shard).Encode(filterCols)
		if err := s.broker.PublishWithHeaders(key, body, headers); err != nil {
			// TODO: log
			continue
		}
	}

	return nil
}

func (s SenderShard) Eof(eof protocol.Eof) error {
	body := eof.Encode()
	headers := amqp.Table{
		"type": protocol.EOF,
	}
	return s.broadcast(body, headers)
}

func (s SenderShard) Error(erro protocol.Error) error {
	body := erro.Encode()
	headers := amqp.Table{
		"type": protocol.ERROR,
	}
	return s.broadcast(body, headers)
}

func (s SenderShard) broadcast(body []byte, headers amqp.Table) error {
	for i := range s.n {
		key := fmt.Sprintf(s.fmt, i)
		if err := s.broker.PublishWithHeaders(key, body, headers); err != nil {
			// TODO: log
			continue
		}
	}

	return nil
}
