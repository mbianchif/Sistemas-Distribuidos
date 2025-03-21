package common

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// Message kind
const (
    KIND_BATCH = 0
    KIND_CONFIRM = 1
)

const DELIMITER = ","
const BET_DELIMITER = ";"
const BATCH_SIZE_SIZE = 4
const BATCH_COUNT_SIZE = 4
const ID_SIZE = 1
const MESSAGE_KIND_SIZE = 1
const WINNER_COUNT_SIZE = 4

type Bet struct {
	Agency    string
	Name      string
	Surname   string
	Id        string
	Birthdate string
	Number    string
}

func (m Bet) Encode() []byte {
	fields := []string{
		m.Agency,
		m.Name,
		m.Surname,
		m.Id,
		m.Birthdate,
		m.Number,
	}

	return []byte(strings.Join(fields, DELIMITER))
}

type BetSockStream struct {
	conn net.Conn
	id   int
}

func BetSockConnect(address string, id string) (*BetSockStream, error) {
    parsedID, err := strconv.Atoi(id)
    if err != nil {
        return nil, err
    }

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

    // Share id with the other end
    writer := bufio.NewWriter(conn)
    writer.Write([]byte(id))
    if err := writer.Flush(); err != nil {
        conn.Close()
        return nil, err
    }

	return &BetSockStream{conn, parsedID}, nil
}

func (s BetSockStream) PeerAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func Batch(arr []Bet, r int) [][]Bet {
	res := make([][]Bet, 0)
	for i := 0; i < len(arr); i += r {
		start := i
		end := minInt(len(arr), i+r)
		res = append(res, arr[start:end])
	}
	return res
}

func (s *BetSockStream) SendBets(bets []Bet, batchSize int) error {
	writer := bufio.NewWriter(s.conn)
	batches := Batch(bets, batchSize)

    // Write message kind
    writer.Write([]byte{KIND_BATCH})

	// Write batch count
	nbatches := len(batches)
	nbatchesBytes := make([]byte, BATCH_COUNT_SIZE)
	binary.BigEndian.PutUint32(nbatchesBytes, uint32(nbatches))
	writer.Write(nbatchesBytes)

	for _, batch := range batches {
		betsEncoded := make([][]byte, 0)
		for _, bet := range batch {
			betsEncoded = append(betsEncoded, bet.Encode())
		}
		batchBytes := bytes.Join(betsEncoded, []byte(BET_DELIMITER))

		// Write batch size and data
		batchSize := len(batchBytes)
		batchSizeBytes := make([]byte, BATCH_SIZE_SIZE)
		binary.BigEndian.PutUint32(batchSizeBytes, uint32(batchSize))
		writer.Write(batchSizeBytes)
		writer.Write(batchBytes)
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("couldn't send message: %v", err)
	}

	return nil
}

func (s *BetSockStream) Confirm() error {
    writer := bufio.NewWriter(s.conn)

    // Write message kind
    writer.Write([]byte{KIND_CONFIRM})
    
    if err := writer.Flush(); err != nil {
        return fmt.Errorf("couldn't send confirmation")
    }

    return nil
}

func (s *BetSockStream) RecvWinners() (int, error) {
    countBytes := make([]byte, WINNER_COUNT_SIZE)

    n, err := io.ReadFull(s.conn, countBytes)
    if err != nil {
        return 0, fmt.Errorf("couldn't recv winner quantity, err: %v, read %v out of %v bytes", err, n, WINNER_COUNT_SIZE)
    }

    return n, nil
}

func (s *BetSockStream) Close() {
	s.conn.Close()
}
