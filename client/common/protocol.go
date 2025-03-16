package common

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

const DELIMITER = ","
const TERMINATOR = ";"
const BATCH_SIZE_SIZE = 4

type Message struct {
	Agency    string
	Name      string
	Surname   string
	Id        string
	Birthdate string
	Number    string
}

func (m Message) Encode() []byte {
	fields := []string{
		m.Agency,
		m.Name,
		m.Surname,
		m.Id,
		m.Birthdate,
		m.Number,
	}

	return []byte(strings.Join(fields, DELIMITER) + TERMINATOR)
}

type BetSockStream struct {
	conn net.Conn
}

func BetSockConnect(address string) (*BetSockStream, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &BetSockStream{conn}, nil
}

func (s BetSockStream) PeerAddr() net.Addr {
	return s.conn.RemoteAddr()
}

func (s *BetSockStream) Send(msgs ...Message) error {
    encodedMsgs := make([][]byte, 0, len(msgs))
    for _, msg := range msgs {
        encodedMsgs = append(encodedMsgs, msg.Encode())
    }
    encoded := bytes.Join(encodedMsgs, []byte(TERMINATOR))

	batchSize := len(encoded)
	batchSizeBytes := make([]byte, BATCH_SIZE_SIZE)
	binary.BigEndian.PutUint32(batchSizeBytes, uint32(batchSize))

	writer := bufio.NewWriter(s.conn)
	writer.Write(batchSizeBytes)
	writer.Write(encoded)

	err := writer.Flush()
	if err != nil {
		return fmt.Errorf("couldn't send message: %v", err)
	}

	return nil
}

func (s *BetSockStream) Close() {
	s.conn.Close()
}
