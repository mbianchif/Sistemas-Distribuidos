package rabbit

import (
	"fmt"

	"analyzer/comms"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SenderShard struct {
	broker       *Broker
	fmt          string
	key          string
	outputCopies int
	log          *logging.Logger
	seq          []int
}

func NewShard(broker *Broker, fmt string, key string, outputCopies int, log *logging.Logger) *SenderShard {
	return &SenderShard{
		broker:       broker,
		fmt:          fmt,
		key:          key,
		outputCopies: outputCopies,
		log:          log,
		seq:          make([]int, outputCopies),
	}
}

func keyHash(str string, mod int) int {
	var hash uint64 = 5381

	for _, c := range str {
		hash = ((hash << 5) + hash) + uint64(c) // hash * 33 + c
	}

	return int(hash % uint64(mod))
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

func (s *SenderShard) nextKeySeq(i int) (string, int) {
	seq := s.seq[i]
	s.seq[i] += 1
	return fmt.Sprintf(s.fmt, i), seq
}

func (s *SenderShard) Batch(batch comms.Batch, filterCols map[string]struct{}, headers amqp.Table) error {
	shards, err := s.shard(batch.FieldMaps)
	if err != nil {
		return err
	}

	for i, shard := range shards {
		key, seq := s.nextKeySeq(i)
		body := comms.NewBatch(shard).Encode(filterCols)
		headers["seq"] = seq
		if err := s.broker.Publish(key, body, headers); err != nil {
			s.log.Errorf("error while publishing sharded message to %v", i)
			continue
		}
	}

	return nil
}

func (s *SenderShard) Eof(eof comms.Eof, headers amqp.Table) error {
	body := eof.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderShard) Broadcast(body []byte, headers amqp.Table) error {
	for i := range s.outputCopies {
		key, seq := s.nextKeySeq(i)
		headers["seq"] = seq
		if err := s.broker.Publish(key, body, headers); err != nil {
			return err
		}
	}
	return nil
}
