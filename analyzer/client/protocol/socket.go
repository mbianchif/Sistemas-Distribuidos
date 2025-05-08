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
	"strconv"
	"sync"

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

func (s *CsvTransferStream) sendBatch(batch []byte, headerSize int) error {
	binary.BigEndian.PutUint32(batch[1:], uint32(len(batch)-headerSize))
	return writeAll(s.conn, batch)
}

func fits(currentSize int, csvRowSize int, batchSize int) bool {
	return currentSize+1+csvRowSize <= batchSize
}

func (s *CsvTransferStream) SendFile(fp *os.File, fileId uint8, batchSize int) error {
	if err := writeAll(s.conn, []byte{fileId}); err != nil {
		return fmt.Errorf("couldn't send fileId %d: %v", fileId, err)
	}

	reader := csv.NewReader(bufio.NewReader(fp))
	reader.Read() // Skip header line

	headerSize := 5 // 1:type + 4:dataSize
	records := make([]byte, headerSize, batchSize)
	records[0] = MSG_BATCH

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
			if !fits(len(records), size, batchSize) {
				if len(records) == 0 {
					return fmt.Errorf("BATCH_SIZE should be incremented to let record of size %dB through", size)
				}

				if err := s.sendBatch(records, headerSize); err != nil {
					return fmt.Errorf("couldn't send batch with fileId %d: %v", fileId, err)
				}

				records = records[:headerSize]
			}

			if len(records) > headerSize {
				records = append(records, []byte("\n")...)
			}
			records = append(records, buf.Bytes()...)
		}
	}

	if len(records) > 0 {
		if err := s.sendBatch(records, headerSize); err != nil {
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

func spawnFileHandler(query int, ch <-chan []byte, storage string) error {
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
		writeAll(writer, append(data, []byte("\n")...))
	}

	return writer.Flush()
}

func (s *CsvTransferStream) RecvQueryResult(storage string, queryCount int) {
	wg := &sync.WaitGroup{}
	wg.Add(queryCount)

	chans := make(map[int]chan<- []byte, queryCount)
	for i := 1; i <= queryCount; i++ {
		ch := make(chan []byte)
		chans[i] = ch
		go func(i int) {
			defer wg.Done()
			if err := spawnFileHandler(i, ch, storage); err != nil {
				s.log.Criticalf("error at spawnFileHandler: %v", err)
			}
		}(i)
	}

	headerSize := 6
	header := make([]byte, headerSize)
	for doneQueries := 0; doneQueries < queryCount; {
		n, err := io.ReadFull(s.conn, header)
		if err != nil || n < headerSize {
			s.log.Error("failed to recv message from gateway")
			break
		}

		dataLength := binary.BigEndian.Uint32(header)
		kind := int(header[4])
		query := int(header[5])
		switch kind {
		case MSG_BATCH:
			data := make([]byte, dataLength)
			n, err = io.ReadFull(s.conn, data)
			if err != nil || n < int(dataLength) {
				continue
			}
			chans[query] <- data
			continue

		case MSG_EOF:
			s.log.Infof("Query %v is ready!", query)

		case MSG_ERR:
			s.log.Errorf("an error occurred with query %v", query)
		}

		doneQueries++
		close(chans[query])
	}

	wg.Wait()
}

func (s *CsvTransferStream) Close() {
	if s != nil && s.conn != nil {
		s.conn.Close()
	}
}
