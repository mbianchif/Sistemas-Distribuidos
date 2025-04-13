package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
)

const (
	MSG_BATCH = iota
	MSG_FIN
	MSG_ERR
)

type CsvTransferStream struct {
	conn net.Conn
}

func Connect(ip string, port uint16) (*CsvTransferStream, error) {
	addr := net.JoinHostPort(ip, strconv.Itoa(int(port)))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &CsvTransferStream{conn}, nil
}

func (s *CsvTransferStream) sendBatch(writer *bufio.Writer, lines [][]byte) error {
	writer.WriteByte(MSG_BATCH)
	err := writer.Flush()
	if err != nil {
		return fmt.Errorf("couldn't send batch")
	}

	quantBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(quantBytes, uint32(len(lines)))
	writer.Write(quantBytes)

	lengthBytes := make([]byte, 4)
	for _, line := range lines {
		binary.BigEndian.PutUint32(lengthBytes, uint32(len(line)))
		writer.Write(lengthBytes)
		writer.Write(line)
	}

	return writer.Flush()
}

func (s *CsvTransferStream) SendFile(fp *os.File, fileId uint8, batchSize int) error {
	writer := bufio.NewWriter(s.conn)
	writer.WriteByte(fileId)
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("couldn't send file id")
	}

	reader := bufio.NewReader(fp)
	lines := make([][]byte, 0, batchSize)

	var line []byte
	var err error
	for {
		line, err = reader.ReadBytes('\n')
		if err == io.EOF {
			err = nil
			if len(line) > 0 {
				lines = append(lines, line[:len(line)-1])
			}
			if len(lines) > 0 {
				err = s.sendBatch(writer, lines)
			}
			break
		}

		if err != nil {
			break
		}

		// Append line without \n
		lines = append(lines, line[:len(line)-1])
		if len(lines) == batchSize {
			err = s.sendBatch(writer, lines)
			if err != nil {
				break
			}
			lines = lines[:0]
		}
	}

	if err != nil {
		writer.WriteByte(MSG_ERR)
	} else {
		writer.WriteByte(MSG_FIN)
	}

	if writer.Flush() != nil {
		return fmt.Errorf("couldn't flush error")
	}

	return err
}

func (s *CsvTransferStream) RecvQueryResult(storage string) (int, error) {
	return 0, nil
}

func (s *CsvTransferStream) Close() {
	if s != nil && s.conn != nil {
		s.conn.Close()
	}
}
