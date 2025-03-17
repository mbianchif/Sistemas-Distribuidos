package common

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

const MSG_SIZE_SIZE = 4
const DELIMITER = "\n"
const TERMINATOR = ';'

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

func (s *BetSockStream) Send(bet Bet) error {
	writer := bufio.NewWriter(s.conn)
	writer.Write(bet.Encode())

	err := writer.Flush()
	if err != nil {
		return fmt.Errorf("couldn't send message: %v", err)
	}
	return nil
}

func (s *BetSockStream) Close() {
	s.conn.Close()
}
