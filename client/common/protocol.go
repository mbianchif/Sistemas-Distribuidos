package common

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"
)

const DELIMITER = ','
const TERMINATOR = ';'
const BATCH_TERMINATOR = '\n'

type Message struct {
	Agency    string
	Name      string
	Surname   string
	Id        string
	Birthdate string
	Number    string
}

func MsgFromBytes(data []byte) Message {
	fields := strings.Split(string(bytes.TrimRight(data, string(TERMINATOR))), string(DELIMITER))
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

	data := []byte(strings.Join(fields, string(DELIMITER)))
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

func (s *BetSockStream) Send(msgs ...Message) error {
	writer := bufio.NewWriter(s.conn)

	for _, msg := range msgs {
		writer.Write(msg.Encode())
	}
	writer.WriteByte(BATCH_TERMINATOR)

	err := writer.Flush()
	if err != nil {
		return fmt.Errorf("couldn't send message: %v", err)
	}

	return nil
}

func (s *BetSockStream) Recv() ([]Message, error) {
	data, err := bufio.NewReader(s.conn).ReadBytes(BATCH_TERMINATOR)
	if err != nil {
		return nil, fmt.Errorf("couldn't recv message: %v", err)
	}

	msgChunks := bytes.Split(data, []byte{TERMINATOR})
	msgs := make([]Message, 0)

	for _, chunk := range msgChunks {
		msgs = append(msgs, MsgFromBytes(bytes.TrimRight(chunk, string(BATCH_TERMINATOR))))
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
