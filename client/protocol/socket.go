package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

const (
	MSG_BATCH = iota
	MSG_EOF
	MSG_ERR
)

type CsvTransferStream struct {
	conn net.Conn
	log  *logging.Logger
}

func NewConnection(ip string, port uint16, log *logging.Logger) (*CsvTransferStream, error) {
	addr := net.JoinHostPort(ip, strconv.Itoa(int(port)))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("Couldn't connect with ip %v in port %v", ip, port)
		return nil, err
	}
	return &CsvTransferStream{conn, log}, nil
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
		if nlines != -1 && sentLines+len(lines) == nlines {
			if len(lines) > 0 {
				err = s.sendBatch(writer, lines)
			}
			break
		}

		// batchSize lines were read
		if len(lines) == batchSize {
			err = s.sendBatch(writer, lines)
			if err != nil {
				s.log.Errorf("Error sending batch")
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
		writer.WriteByte(MSG_EOF)
	}

	return writer.Flush()
}

func writeAll(w *bufio.Writer, data []byte) error {
	total := 0
	for total < len(data) {
		n, err := w.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}
	return nil
}

func spawn_file_handler(s *CsvTransferStream, query int, ch <-chan []byte, storage string) error {
	if strings.Contains(storage, "0") {
		return fmt.Errorf("una pena")
	}
	// path := storage + "/" + strconv.Itoa(query) + ".csv"
	// // fp, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	// if err != nil {
	// 	return fmt.Errorf("the file could not be created %v", err)
	// }
	// defer fp.Close()

	// writer := bufio.NewWriterSize(fp, 1<<12)
	for data := range ch {
		for line := range bytes.SplitSeq(data, []byte("\n")) {
			s.log.Infof("query %v, line: `%s`", query, line)
		}
		// data = append(data, []byte("\n")...)
		// writeAll(writer, data)
		// writer.Flush()
	}

	return nil
}

func (s *CsvTransferStream) RecvQueryResult(storage string) {
	queryCount := 5

	chans := make(map[int]chan<- []byte, queryCount)
	for i := 1; i <= queryCount; i++ {
		ch := make(chan []byte)
		chans[i] = ch
		go func(i int) {
			spawn_file_handler(s, i, ch, storage)
		}(i)
	}

	headerSize := 6
	done := make(map[int]struct{}, queryCount)
	header := make([]byte, headerSize)
	for len(done) < queryCount {
		n, err := io.ReadFull(s.conn, header)
		if err != nil || n < headerSize {
			s.log.Error("failed to recv message from gateway")
			break
		}

		dataLength := binary.BigEndian.Uint32(header[:4])
		kind := int(header[4])
		query := int(header[5])
		if kind == MSG_EOF {
			s.log.Infof("query %v is ready!")
			done[query] = struct{}{}
			close(chans[query])
			continue
		} else if kind == MSG_ERR {
			s.log.Errorf("an error occurred with query %v", query)
			done[query] = struct{}{}
			close(chans[query])
			continue
		}

		data := make([]byte, dataLength)
		n, err = io.ReadFull(s.conn, data)
		if err != nil || n < int(dataLength) {
			continue
		}

		chans[query] <- data
	}
}

func (s *CsvTransferStream) Close() {
	if s != nil && s.conn != nil {
		s.conn.Close()
	}
}
