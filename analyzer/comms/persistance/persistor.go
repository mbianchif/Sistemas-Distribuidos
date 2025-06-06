package persistance

import (
	"analyzer/comms"
	"analyzer/comms/middleware"
	"bytes"
	"fmt"
	"iter"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type PersistedFile struct {
	ReplicaId int
	Seq       int
	ClientId  int
	FileName  string
	State     []byte
}

type Persistor struct {
	lastDeliveries map[int]middleware.DelId
	dirName        string
	log            *logging.Logger
}

func New(dirName string, log *logging.Logger) Persistor {
	return Persistor{
		lastDeliveries: make(map[int]middleware.DelId),
		dirName:        dirName,
		log:            log,
	}
}

func buildHeader(replicaId, seq int) []byte {
	return fmt.Appendf(nil, "%d %d\n", replicaId, seq)
}

func (p Persistor) Store(id middleware.DelId, fileName string, data []byte) error {
	header := buildHeader(id.ReplicaId, id.Seq)
	dirPath := fmt.Sprintf("%s/%d", p.dirName, id.ClientId)
	p.lastDeliveries[id.ClientId] = id
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

func (p Persistor) Load(clientId int, fileName string) (int, int, []byte, error) {
	dirPath := fmt.Sprintf("%s/%d", p.dirName, clientId)
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

	return replicaId, seq, fields[1], nil
}

func (p Persistor) RecoverFor(clientId int) (iter.Seq[PersistedFile], error) {
	return func(yield func(PersistedFile) bool) {
		files, err := os.ReadDir(fmt.Sprintf("%s/%d", p.dirName, clientId))
		if err != nil {
			p.log.Errorf("failed to read files in directory %d: %v", clientId, err)
			return
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			name := file.Name()
			if strings.HasSuffix(name, ".tmp") {
				continue
			}

			replicaId, seq, state, err := p.Load(clientId, name)
			if err != nil {
				p.log.Errorf("failed to load file %s for client %d: %v", file.Name(), clientId, err)
				continue
			}

			p.lastDeliveries[clientId] = middleware.DelId{
				ReplicaId: replicaId,
				Seq:       seq,
				ClientId:  clientId,
			}

			pf := PersistedFile{
				ReplicaId: replicaId,
				ClientId:  clientId,
				Seq:       seq,
				FileName:  name,
				State:     state,
			}

			if !yield(pf) {
				return
			}
		}
	}, nil
}

func (p Persistor) Recover() (iter.Seq[PersistedFile], error) {
	files, err := os.ReadDir(p.dirName)
	if err != nil {
		return slices.Values([]PersistedFile{}), nil
	}

	return func(yield func(PersistedFile) bool) {
		for _, clientDir := range files {
			if !clientDir.IsDir() {
				continue
			}

			clientId, err := strconv.Atoi(clientDir.Name())
			if err != nil {
				p.log.Errorf("failed to parse clientId from directory name %s: %v", clientDir.Name(), err)
				continue
			}

			files, err := p.RecoverFor(clientId)
			for file := range files {
				if !yield(file) {
					return
				}
			}
		}
	}, nil
}

func (p *Persistor) IsDup(del middleware.Delivery) bool {
	clientId := del.Headers.ClientId
	replicaId := del.Headers.ReplicaId
	seq := del.Headers.Seq

	if lastDelId, ok := p.lastDeliveries[clientId]; ok {
		if seq <= lastDelId.Seq && replicaId == lastDelId.ReplicaId {
			p.log.Debugf("client %v: delivery is already processed, skipping", clientId)
			return true
		}
	}

	return false
}
