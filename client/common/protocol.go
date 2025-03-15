package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
)

const MSG_SIZE_SIZE = 4

type Message struct {
	Agency    string
	Name      string
	Surname   string
	Id        string
	Birthdate string
	Number    string
}

func MsgFromBytes(data []byte) Message {
	fields := strings.Split(string(data), "\n")
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

	return []byte(strings.Join(fields, "\n"))
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

func (s *BetSockStream) sendAll(data []byte) int {
	written := 0
	for written < len(data) {
		n, err := s.conn.Write(data[written:])
		if err != nil {
			return written
		}
		written += n
	}
	return written
}

func (s *BetSockStream) Send(msg Message) error {
	data := msg.Encode()

	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(data)))
	if len(length) != s.sendAll(length) {
		return fmt.Errorf("couldn't send the message length")
	}

	if len(data) != s.sendAll(data) {
		return fmt.Errorf("couldn't send the message data")
	}

	return nil
}

func (s *BetSockStream) recvAll(n int) ([]byte, error) {
	buff := make([]byte, n)
    readTotal := 0
	for len(buff) < n {
        read, err := s.conn.Read(buff[readTotal:])
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			break
		}
        readTotal += read

	}
    return buff[:readTotal], nil
}

func (s *BetSockStream) Recv() (Message, error) {
	sizeBytes, err := s.recvAll(MSG_SIZE_SIZE)
	if err != nil {
		return Message{}, err
	}

	size := binary.BigEndian.Uint32(sizeBytes)
	data, err := s.recvAll(int(size))
	return MsgFromBytes(data), nil
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
