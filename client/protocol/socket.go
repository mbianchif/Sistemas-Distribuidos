package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
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

func (s *CsvTransferStream) SendFile(fp *os.File, fileId uint8, batchSize int, nlines int) error {
	writer := bufio.NewWriter(s.conn)
	writer.WriteByte(fileId)

	reader := bufio.NewReader(fp)
	lines := make([][]byte, 0, batchSize)
	sentLines := 0

	// Skip header line
	line, err := reader.ReadBytes('\n')
	for {
		line, err = reader.ReadBytes('\n')
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			lines = append(lines, line)
		}

		// File was read entirely
		if err == io.EOF {
			err = nil
			if len(lines) > 0 {
				err = s.sendBatch(writer, lines)
			}
			break
		}

		// Read the wanted lines of the file
		if nlines != -1 && sentLines + len(lines) == nlines {
			if len(lines) > 0 {
				err = s.sendBatch(writer, lines)
			}
			break
		}

		// batchSize lines were read
		if len(lines) == batchSize {
			err = s.sendBatch(writer, lines)
			if err != nil {
				break
			}
			sentLines += len(lines)
			lines = lines[:0]
			if sentLines == nlines {
				break
			}
		}
	}

	if err != nil {
		writer.WriteByte(MSG_ERR)
	} else {
		writer.WriteByte(MSG_FIN)
	}

	return writer.Flush()
}

func (s *CsvTransferStream) RecvQueryResult(storage string) (int, error) {
	return 0, nil
}

func (s *CsvTransferStream) Close() {
	if s != nil && s.conn != nil {
		s.conn.Close()
	}
}
