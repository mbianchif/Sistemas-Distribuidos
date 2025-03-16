package common

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
)

const BATCH_SIZE_SIZE = 4
const MSG_SIZE_SIZE = 4
const DELIMITER = "\n"
const TERMINATOR = ';'

type Message struct {
	Agency    string
	Name      string
	Surname   string
	Id        string
	Birthdate string
	Number    string
}

func MsgFromBytes(data []byte) Message {
	fields := strings.Split(string(data), DELIMITER)
	return Message{
		fields[0],
		fields[1],
		fields[2],
		fields[3],
		fields[4],
		fields[5],
	}
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

	data := []byte(strings.Join(fields, DELIMITER))
	return append(data, TERMINATOR)
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

func (s *BetSockStream) Send(msgs... Message) error {
	writer := bufio.NewWriter(s.conn)

    // Write batch size
    batchSizeBytes := make([]byte, BATCH_SIZE_SIZE)
    binary.BigEndian.PutUint32(batchSizeBytes, uint32(len(msgs)))
    writer.Write(batchSizeBytes)

    for _, msg := range msgs {
        writer.Write(msg.Encode())
    }

	err := writer.Flush()
	if err != nil {
        return fmt.Errorf("couldn't send message: %v", err)
	}
	return nil
}

func (s *BetSockStream) Recv() ([]Message, error) {
    reader := bufio.NewReader(s.conn)

    // Read batch size
    batchSizeBytes := make([]byte, BATCH_SIZE_SIZE)
    io.ReadFull(reader, batchSizeBytes)
    batchSize := int(binary.BigEndian.Uint32(batchSizeBytes))

    msgs := make([]Message, 0, batchSize)
    for i := 0; i < batchSize; i++ {
        data, err := reader.ReadBytes(TERMINATOR)
        if err != nil {
            return msgs, fmt.Errorf("batch got cut in the middle: %v", err)
        }
        msgs = append(msgs, MsgFromBytes(data))
    }

    return msgs, nil
}

func (s *BetSockStream) Close() {
	s.conn.Close()
}

type BetSockListener struct {
	listener net.Listener
}

func BetSockBind(host string, port int, backlog int) (*BetSockListener, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf("%v:%v", host, port))
	if err != nil {
		return nil, err
	}
	return &BetSockListener{listener}, nil
}

func (l *BetSockListener) Accept() (*BetSockStream, error) {
	skt, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}
	return &BetSockStream{skt}, nil
}

func (l *BetSockListener) Close() {
	l.listener.Close()
}
