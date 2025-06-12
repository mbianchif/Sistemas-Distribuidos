package middleware

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"analyzer/comms"

	"github.com/op/go-logging"
)

type SenderShard struct {
	broker       *Broker
	log          *logging.Logger
	outputCopies int
	keys         []string
	fmt          string

	// Persisted
	seq []map[int]int
}

func NewShard(broker *Broker, qFmt string, keys []string, outputCopies int, log *logging.Logger) *SenderShard {
	seq := make([]map[int]int, outputCopies)
	for i := range seq {
		seq[i] = make(map[int]int)
	}

	return &SenderShard{
		broker:       broker,
		fmt:          qFmt,
		keys:         keys,
		outputCopies: outputCopies,
		log:          log,
		seq:          seq,
	}
}

func (s *SenderShard) nextKeySeq(i int, clientId int) (string, int) {
	key := fmt.Sprintf(s.fmt, i)
	seq := s.seq[i][clientId]

	s.seq[i][clientId]++
	return key, seq
}

func keyHash(str string) uint64 {
	var hash uint64 = 5381

	for _, c := range str {
		hash = ((hash << 5) + hash) + uint64(c) // hash * 33 + c
	}

	return hash
}

func (s *SenderShard) Batch(batch comms.Batch, filterCols map[string]struct{}, headers Table) error {
	shards, err := comms.Shard(batch.FieldMaps, s.keys, func(str string) int {
		return int(keyHash(str) % uint64(s.outputCopies))
	})
	if err != nil {
		return err
	}

	clientId := int(headers["client-id"].(int32))

	for i, shard := range shards {
		key, seq := s.nextKeySeq(i, clientId)
		body := comms.NewBatch(shard).Encode(filterCols)
		headers["seq"] = seq
		if err := s.broker.Publish(key, body, headers); err != nil {
			s.log.Errorf("error while publishing sharded message to %d: %v", i, err)
			return err
		}
	}

	return nil
}

func (s *SenderShard) Eof(eof comms.Eof, headers Table) error {
	body := eof.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderShard) Flush(flush comms.Flush, headers Table) error {
	body := flush.Encode()
	err := s.Broadcast(body, headers)

	clientId := int(headers["client-id"].(int32))
	for replicaId := range s.outputCopies {
		delete(s.seq[replicaId], clientId)
	}

	return err
}

func (s *SenderShard) Purge(purge comms.Purge, headers Table) error {
	body := purge.Encode()
	err := s.Broadcast(body, headers)

	s.seq = make([]map[int]int, s.outputCopies)
	for i := range s.seq {
		s.seq[i] = make(map[int]int)
	}

	return err
}

func (s *SenderShard) Broadcast(body []byte, headers Table) error {
	clientId := int(headers["client-id"].(int32))

	for i := range s.outputCopies {
		key, seq := s.nextKeySeq(i, clientId)
		headers["seq"] = seq
		if err := s.broker.Publish(key, body, headers); err != nil {
			return err
		}
	}

	return nil
}

// Example: "shard <seq> ... <seq>"
func (s *SenderShard) Encode(clientId int) []byte {
	init := []byte("shard")
	builder := bytes.NewBuffer(init)

	for replicaId := range s.seq {
		seq := s.seq[replicaId][clientId]
		builder.WriteRune(' ')
		builder.WriteString(strconv.Itoa(seq))
	}

	return builder.Bytes()
}

// Example: "shard <seq> ... <seq>"
func DecodeLineShard(line string) ([]int, error) {
	line, _ = strings.CutPrefix(line, "shard ")

	parts := strings.Split(line, " ")
	if len(parts) < 1 {
		return nil, fmt.Errorf("the amount of parts is not enough: %s", line)
	}

	// Read sequence numbers
	seqs := make([]int, 0, len(parts))
	for _, seqStr := range parts {
		seq, err := strconv.Atoi(seqStr)
		if err != nil {
			return nil, fmt.Errorf("seq number is not a number: %s", line)
		}
		seqs = append(seqs, seq)
	}

	return seqs, nil
}

func (s *SenderShard) SetState(clientId int, seqs []int) error {
	if len(seqs) != s.outputCopies {
		return fmt.Errorf("expected %d seqs, got %d", s.outputCopies, len(seqs))
	}

	for i, seq := range seqs {
		s.seq[i][clientId] = seq
	}

	return nil
}
