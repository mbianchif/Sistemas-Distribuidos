package middleware

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"analyzer/comms"
)

type SenderRobin struct {
	broker       *Broker
	outputCopies int
	fmt          string

	// Persisted
	cur map[int]int
	seq []map[int]int
}

func NewRobin(broker *Broker, fmt string, outputCopies int) *SenderRobin {
	seq := make([]map[int]int, outputCopies)
	for i := range seq {
		seq[i] = make(map[int]int)
	}

	return &SenderRobin{
		broker:       broker,
		fmt:          fmt,
		outputCopies: outputCopies,
		cur:          make(map[int]int),
		seq:          seq,
	}
}

func (s *SenderRobin) Batch(batch comms.Batch, filterCols map[string]struct{}, headers Table) error {
	body := batch.Encode(filterCols)
	return s.Direct(body, headers)
}

func (s *SenderRobin) Eof(eof comms.Eof, headers Table) error {
	body := eof.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderRobin) Flush(flush comms.Flush, headers Table) error {
	body := flush.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderRobin) nextKeySeq(clientId int) (string, int) {
	i := s.cur[clientId]
	key := fmt.Sprintf(s.fmt, i)
	seq := s.seq[i][clientId]

	s.seq[i][clientId]++
	s.cur[clientId] = (i + 1) % s.outputCopies
	return key, seq
}

func (s *SenderRobin) Direct(body []byte, headers Table) error {
	clientId := int(headers["client-id"].(int32))
	key, seq := s.nextKeySeq(clientId)
	headers["seq"] = seq
	return s.broker.Publish(key, body, headers)
}

func (s *SenderRobin) Broadcast(body []byte, headers Table) error {
	for range s.outputCopies {
		if err := s.Direct(body, headers); err != nil {
			return err
		}
	}
	return nil
}

// Example: "robin <fmt> <cur> <seq>  ... <seq>"
func (s *SenderRobin) Encode(clientId int) []byte {
	init := fmt.Appendf(nil, "robin %d", s.cur[clientId])
	builder := bytes.NewBuffer(init)

	for replicaId := range s.seq {
		seq := s.seq[replicaId][clientId]
		builder.WriteRune(' ')
		builder.WriteString(strconv.Itoa(seq))
	}

	return builder.Bytes()
}

// Example: "robin <cur> <seq>  ... <seq>"
func DecodeLineRobin(line string) (int, []int, error) {
	line, _ = strings.CutPrefix(line, "robin ")

	parts := strings.Split(line, " ")
	if len(parts) < 2 {
		return 0, nil, fmt.Errorf("the amount of parts is not enough: %s", line)
	}

	// Read current index
	cur, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, nil, fmt.Errorf("cur is not a number: %s", line)
	}

	// Read sequence numbers
	seqs := make([]int, 0, len(parts)-1)
	for _, seqStr := range parts[1:] {
		seq, err := strconv.Atoi(seqStr)
		if err != nil {
			return 0, nil, fmt.Errorf("seq number is not a number: %s", line)
		}
		seqs = append(seqs, seq)
	}

	return cur, seqs, nil
}

func (s *SenderRobin) SetState(clientId int, cur int, seqs []int) error {
	if len(seqs) != s.outputCopies {
		return fmt.Errorf("expected %d seqs, got %d", s.outputCopies, len(seqs))
	}

	s.cur[clientId] = cur

	for i := range s.seq {
		s.seq[i][clientId] = seqs[i]
	}

	return nil
}
