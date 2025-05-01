package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	MSG_BATCH = iota
	MSG_EOF
	MSG_ERR
)

// File identifiers
const (
	FILE_MOVIES  = 0
	FILE_CREDITS = 1
	FILE_RATINGS = 2
)

type Message struct {
	Kind int
	Data [][]byte
}

type CsvTransferStream struct {
	conn net.Conn
}

func NewCsvTransferStream(conn net.Conn) *CsvTransferStream {
	return &CsvTransferStream{conn: conn}
}

func (s *CsvTransferStream) Resource() (string, error) {
	fileIdBytes := make([]byte, 1)
	read, err := io.ReadFull(s.conn, fileIdBytes)
	if err != nil || read < len(fileIdBytes) {
		return "", fmt.Errorf("didn't read full %d bytes: %v", len(fileIdBytes), err)
	}
	fileId := int(fileIdBytes[0])

	files := []string{"movies", "credits", "ratings"}
	if fileId < 0 || fileId >= len(files) {
		return "", fmt.Errorf("invalid file ID received: %d", fileId)
	}

	return files[fileId], nil
}

func (s *CsvTransferStream) Recv() (*Message, error) {
	msgKindBytes := make([]byte, 1)
	read, err := io.ReadFull(s.conn, msgKindBytes)
	if err != nil || read < len(msgKindBytes) {
		return nil, fmt.Errorf("didn't read full %d bytes of msg kind: %v", len(msgKindBytes), err)
	}
	msgKind := int(msgKindBytes[0])

	if msgKind != MSG_BATCH {
		return &Message{Kind: msgKind, Data: nil}, nil
	}

	batchSizeBytes := make([]byte, 4)
	read, err = io.ReadFull(s.conn, batchSizeBytes)
	if err != nil || read < len(batchSizeBytes) {
		return nil, fmt.Errorf("didn't read full %d bytes of batch size: %v", len(batchSizeBytes), err)
	}
	batchSize := int(binary.BigEndian.Uint32(batchSizeBytes))

	lenBytes := make([]byte, 4)
	lines := make([][]byte, 0, batchSize)
	for range batchSize {
		read, err := io.ReadFull(s.conn, lenBytes)
		if err != nil || read < len(lenBytes) {
			return nil, fmt.Errorf("didn't read full %d bytes of length: %v", len(lenBytes), err)
		}
		lineLen := int(binary.BigEndian.Uint32(lenBytes))

		line := make([]byte, lineLen)
		read, err = io.ReadFull(s.conn, line)
		if err != nil || read < len(line) {
			return nil, fmt.Errorf("didn't read full %d bytes of line: %v", len(line), err)
		}
		lines = append(lines, line)
	}

	return &Message{Kind: msgKind, Data: lines}, nil
}

func writeAll(writer io.Writer, data []byte) error {
	written := 0
	for written < len(data) {
		n, err := writer.Write(data[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

func (s *CsvTransferStream) Send(data []byte) error {
	return writeAll(s.conn, data)
}

func (cts *CsvTransferStream) Close() error {
	return cts.conn.Close()
}

type CsvTransferListener struct {
	lis net.Listener
}

func Bind(host string, port int, backlog int) (*CsvTransferListener, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("couldn't bind through requested address %s: %v", addr, err)
	}
	return &CsvTransferListener{lis}, nil
}

func (s *CsvTransferListener) Accept() (*CsvTransferStream, net.Addr) {
	conn, err := s.lis.Accept()
	if err != nil {
		return nil, nil
	}
	return NewCsvTransferStream(conn), conn.RemoteAddr()
}

func (s *CsvTransferListener) Close() {
	s.lis.Close()
}
