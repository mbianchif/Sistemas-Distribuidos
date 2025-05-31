package persistance

import (
	"analyzer/comms"
	"bytes"
	"fmt"
	"os"
	"strconv"
)

const (
	PERSISTOR_DIRNAME = "persistors"
)

type Persistor struct {
	decoder func([]byte) (any, error)
}

func New(decoder func([]byte) (any, error)) *Persistor {
	return &Persistor{decoder: decoder}
}

func buildHeader(replicaId, seq int) []byte {
	return fmt.Appendf(nil, "%d %d\n", replicaId, seq)
}

func (p *Persistor) Persist(replicaId, clientId, seq int, fileName string, data []byte) error {
	header := buildHeader(replicaId, seq)
	dirPath := fmt.Sprintf("%s/%d", PERSISTOR_DIRNAME, clientId)
	return comms.AtomicWrite(dirPath, fileName, append(header, data...))
}

func parseHeader(header []byte) (int, int, error) {
	fields := bytes.Split(header, []byte(" "))
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("invalid header format, has size %d, should have size 2", len(fields))
	}

	replicaId, err := strconv.Atoi(string(fields[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse replicaId from header: %w", err)
	}

	seq, err := strconv.Atoi(string(fields[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to parse seq from header: %w", err)
	}

	return replicaId, seq, nil
}

func (p *Persistor) Load(clientId int, fileName string) (int, int, any, error) {
	dirPath := fmt.Sprintf("%s/%d", PERSISTOR_DIRNAME, clientId)
	path := fmt.Sprintf("%s/%s", dirPath, fileName)

	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("failed to read persistor file %s for client %d: %w", path, clientId, err)
	}

	fields := bytes.SplitN(data, []byte("\n"), 2)
	if len(fields) < 2 {
		return 0, 0, nil, fmt.Errorf("invalid persistor file format: %s", path)
	}

	replicaId, seq, err := parseHeader(fields[0])
	if err != nil {
		return 0, 0, nil, fmt.Errorf("failed to parse header in persistor file %s for client %d: %w", path, clientId, err)
	}

	decoded, err := p.decoder(fields[1])
	return replicaId, seq, decoded, err
}
