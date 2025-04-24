package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"

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

func (s *CsvTransferStream) sendBatch(records [][]byte) error {
	writer := bufio.NewWriter(s.conn)
	writer.WriteByte(MSG_BATCH)

	quantBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(quantBytes, uint32(len(records)))
	writer.Write(quantBytes)

	lengthBytes := make([]byte, 4)
	for _, record := range records {
		binary.BigEndian.PutUint32(lengthBytes, uint32(len(record)))
		writer.Write(lengthBytes)
		writer.Write(record)
	}

	return writer.Flush()
}

func fits(currentSize int, csvRowSize int, batchSize int) bool {
	// TYPE + COUNT + CURRSIZE + ROW_SIZE_SIZE + ROW_SIZE
	return 1 + 4 + currentSize + 4 + csvRowSize < batchSize
}

func (s *CsvTransferStream) SendFile(fp *os.File, fileId uint8, batchSize int) error {
	if err := writeAll(s.conn, []byte{fileId}); err != nil {
		return fmt.Errorf("Couldn't send fileId %d: %v", fileId, err)
	}

	reader := csv.NewReader(bufio.NewReader(fp))
	reader.Read() // Skip header line
	records := make([][]byte, 0, batchSize)
	accSize := 0

	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err == csv.ErrFieldCount {
			continue
		}
		if parseErr, ok := err.(*csv.ParseError); ok && parseErr.Err == csv.ErrFieldCount {
			continue
		}

		if len(row) > 0 {
			buf.Reset()
			csvWriter.Write(row)
			csvWriter.Flush()

			size := buf.Len()
			bytes := slices.Clone(buf.Bytes())

			if !fits(accSize, size, batchSize) {
				if len(records) == 0 {
					return fmt.Errorf("BATCH_SIZE should be incremented to let record of size ~%dB through", size)
				}

				if err := s.sendBatch(records); err != nil {
					return fmt.Errorf("couldn't send batch with fileId %d: %v", fileId, err)
				}

				accSize = 0
				records = records[:0]
			}

			accSize += size
			records = append(records, bytes)
		}
	}

	if len(records) > 0 {
		if err := s.sendBatch(records); err != nil {
			return fmt.Errorf("couldn't send batch with fileId %d: %v", fileId, err)
		}
	}

	return nil
}

func (s *CsvTransferStream) Confirm() error {
	return writeAll(s.conn, []byte{MSG_EOF})
}

func (s *CsvTransferStream) Error() error {
	return writeAll(s.conn, []byte{MSG_ERR})
}

func writeAll(w io.Writer, data []byte) error {
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

func spawn_file_handler(query int, ch <-chan []byte, storage string) error {
	dirPath := fmt.Sprintf("/%s", storage)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	path := fmt.Sprintf("%s/%d.csv", dirPath, query)
	fp, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("the file could not be created %v", err)
	}
	defer fp.Close()

	writer := bufio.NewWriter(fp)
	for data := range ch {
		data = append(data, []byte("\n")...)
		writeAll(writer, data)
		writer.Flush()
	}

	return nil
}

func (s *CsvTransferStream) RecvQueryResult(storage string, queryCount int) {
	chans := make(map[int]chan<- []byte, queryCount)
	for i := 1; i <= queryCount; i++ {
		ch := make(chan []byte)
		chans[i] = ch
		go func(i int) {
			if err := spawn_file_handler(i, ch, storage); err != nil {
				s.log.Criticalf("error at spawn_file_handler: %v", err)
			}
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
			s.log.Infof("query %v is ready!", query)
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
