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

	for _, line := range lines {
		if len(line) == 0 || line[len(line)-1] != '\n' {
			line = append(line, '\n')
		}
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
			if len(line) > 0 {
				lines = append(lines, line)
			}
			if len(lines) > 0 {
				err = s.sendBatch(writer, lines)
				if err != nil {
					break
				}
			}
			err = nil
			break
		}

		if err != nil {
			break
		}

		lines = append(lines, line)
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
		errFlush := writer.Flush()
		if errFlush != nil {
			return fmt.Errorf("couldn't send error")
		}
		return err
	}

	writer.WriteByte(MSG_FIN)
	return writer.Flush()
}

func (s *CsvTransferStream) RecvQueryResult(storage string) (int, error) {
	return 0, nil
}

func (s *CsvTransferStream) Close() {
	s.conn.Close()
}
