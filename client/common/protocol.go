package common

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

const BET_SIZE_SIZE = 4
const DELIMITER = ","

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
    betBytes := bet.Encode()
    betSize := len(betBytes)

    // Write bet size and data
    betSizeBytes := make([]byte, BET_SIZE_SIZE)
    binary.BigEndian.PutUint32(betSizeBytes, uint32(betSize))
    writer.Write(betSizeBytes)
    writer.Write(betBytes)

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("couldn't send message: %v", err)
    }

	return nil
}

func (s *BetSockStream) Close() {
	s.conn.Close()
}
