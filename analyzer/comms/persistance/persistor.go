package persistance

import (
	"bufio"
	"bytes"
	"fmt"
	"iter"
	"os"
	"slices"
	"strconv"
	"strings"

	"analyzer/comms"
	"analyzer/comms/middleware"

	"github.com/op/go-logging"
)

type PersistedHeader struct {
	Seqs []int
}

func (h PersistedHeader) IsDup(id middleware.DelId) bool {
	replicaId := id.ReplicaId
	seq := id.Seq
	return seq <= h.Seqs[replicaId]
}

type PersistedFile struct {
	ClientId int
	FileName string
	Header   PersistedHeader
	State    []byte
}

type Persistor struct {
	dirName  string
	replicas int
	log      *logging.Logger
}

func New(dirName string, replicas int, log *logging.Logger) Persistor {
	return Persistor{
		dirName:  dirName,
		replicas: replicas,
		log:      log,
	}
}

func emptySeqs(n int) []int {
	seqs := make([]int, n)
	for i := range n {
		seqs[i] = -1
	}
	return seqs
}

func (p Persistor) LoadHeader(clientId int, fileName string) (PersistedHeader, error) {
	empty := PersistedHeader{
		Seqs: emptySeqs(p.replicas),
	}

	dirPath := fmt.Sprintf("/%s/%d", p.dirName, clientId)
	path := fmt.Sprintf("%s/%s", dirPath, fileName)
	fp, err := os.Open(path)
	if err != nil {
		return empty, fmt.Errorf("couldn't read header of %s for client %d: %v", fileName, clientId, err)
	}

	seqs := make([]int, 0, p.replicas)
	reader := bufio.NewReader(fp)
	for replicaId := range p.replicas {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return empty, fmt.Errorf("couldn't read lines of header of %s for clinet %d: %v", fileName, clientId, err)
		}

		seq, err := p.parseHeaderLine(line)
		if err != nil {
			return empty, fmt.Errorf("couldn't parse seq number of header for replica %d client %d in file %s: %v", replicaId, clientId, fileName, err)
		}

		seqs = append(seqs, seq)
	}

	return PersistedHeader{seqs}, nil
}

func (p Persistor) parseHeaderLine(header []byte) (int, error) {
	header = bytes.TrimSpace(header)
	seq, err := strconv.Atoi(string(header))
	if err != nil {
		return 0, fmt.Errorf("failed to parse seq number from header: %v", err)
	}

	return seq, nil
}

func (p Persistor) parseHeader(header [][]byte) (PersistedHeader, error) {
	empty := PersistedHeader{}

	seqs := make([]int, 0, p.replicas)
	for _, line := range header {
		seq, err := p.parseHeaderLine(line)
		if err != nil {
			return empty, err
		}

		seqs = append(seqs, seq)
	}

	return PersistedHeader{seqs}, nil
}

func (p Persistor) encodeHeader(header PersistedHeader) []byte {
	buf := bytes.NewBuffer(nil)

	for _, seq := range header.Seqs {
		str := fmt.Sprintf("%d\n", seq)
		buf.WriteString(str)
	}

	return buf.Bytes()
}

func (p Persistor) Store(id middleware.DelId, fileName string, data []byte, headers ...PersistedHeader) error {
	replicaId := id.ReplicaId
	clientId := id.ClientId
	seq := id.Seq

	header := PersistedHeader{}
	if len(headers) > 1 {
		return fmt.Errorf("invalid argument count in persistor.Store")
	} else if len(headers) == 1 && len(headers[0].Seqs) == p.replicas {
		header = headers[0]
	} else {
		header, _ = p.LoadHeader(clientId, fileName)
	}

	header.Seqs[replicaId] = seq
	encodedHeader := p.encodeHeader(header)

	dirPath := fmt.Sprintf("/%s/%d", p.dirName, id.ClientId)
	return comms.AtomicWrite(dirPath, fileName, append(encodedHeader, data...))
}

func (p Persistor) Load(clientId int, fileName string) (PersistedFile, error) {
	dirPath := fmt.Sprintf("/%s/%d", p.dirName, clientId)
	path := fmt.Sprintf("%s/%s", dirPath, fileName)
	empty := PersistedFile{}

	data, err := os.ReadFile(path)
	if err != nil {
		return empty, fmt.Errorf("failed to read persistor file %s for client %d: %v", fileName, clientId, err)
	}

	fields := bytes.SplitN(data, []byte("\n"), p.replicas+1)

	header, err := p.parseHeader(fields[:p.replicas])
	if err != nil {
		return empty, fmt.Errorf("failed to parse header at file %s for client %d: %v", fileName, clientId, err)
	}

	state := fields[len(fields)-1]

	return PersistedFile{
		ClientId: clientId,
		FileName: fileName,
		Header:   header,
		State:    state,
	}, nil
}

func (p Persistor) RecoverFor(clientId int) (iter.Seq[PersistedFile], error) {
	dirPath := fmt.Sprintf("/%s/%d", p.dirName, clientId)
	files, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read files in directory %d: %v", clientId, err)
	}

	return func(yield func(PersistedFile) bool) {
		for _, file := range files {
			if file.IsDir() {
				continue
			}

			name := file.Name()
			if strings.HasSuffix(name, ".tmp") {
				continue
			}

			pf, err := p.Load(clientId, name)
			if err != nil {
				p.log.Errorf("failed to load file %s for client %d: %v", file.Name(), clientId, err)
				continue
			}

			if !yield(pf) {
				return
			}
		}
	}, nil
}

func (p Persistor) Recover() (iter.Seq[PersistedFile], error) {
	dirPath := fmt.Sprintf("/%s", p.dirName)
	files, err := os.ReadDir(dirPath)
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

func (p Persistor) Flush(clientId int) error {
	dirPath := fmt.Sprintf("/%s/%d", p.dirName, clientId)
	return os.RemoveAll(dirPath)
}

func (p Persistor) Purge() error {
	dirPath := fmt.Sprintf("/%s", p.dirName)
	return os.RemoveAll(dirPath)
}
