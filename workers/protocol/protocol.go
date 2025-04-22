package protocol

import (
	"bytes"
	"fmt"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

var name2Id = map[string]int{
	// movies
	"id":                   0,
	"title":                1,
	"release_date":         2,
	"overview":             3,
	"budget":               4,
	"revenue":              5,
	"genres":               6,
	"production_countries": 7,
	"spoken_languages":     8,

	// ratings
	"movieId":   9,
	"rating":    10,
	"timestamp": 11,

	// credits
	"cast": 12,

	// Added
	"rate_revenue_budget": 13,
	"sentiment":           14,
	"country":             15,
	"actor":               16,
	"count":               17,
}

var id2Name = []string{
	// movies
	"id",
	"title",
	"release_date",
	"overview",
	"budget",
	"revenue",
	"genres",
	"production_countries",
	"spoken_languages",

	// ratings
	"movieId",
	"rating",
	"timestamp",

	// credits
	"cast",

	// Added
	"rate_revenue_budget",
	"sentiment",
	"country",
	"actor",
	"count",
}

const (
	BATCH = iota
	EOF
	ERROR
)

/*

1 Tipo
	- Batch
		- Payload
			\n separated
	- EOF
*/

func ReadDelivery(del amqp.Delivery) (int, []byte) {
	body := del.Body
	if len(body) < 1 {
		return ERROR, nil
	}
	return int(body[0]), body[1:]
}

type Batch struct {
	FieldMaps []map[string]string
}

func NewBatch(fieldMaps []map[string]string) Batch {
	return Batch{fieldMaps}
}

func encodeLine(fields map[string]string, filterCols map[string]struct{}) []byte {
	it := 0
	bytes := make([]byte, 0, 512)
	for k, v := range fields {
		if len(filterCols) > 0 {
			if _, ok := filterCols[k]; !ok {
				it += 1
				continue
			}
		}

		if _, ok := name2Id[k]; !ok {
			panic(fmt.Sprintf("field %v is not supported by protocol, must add", k))
		}

		kId := strconv.Itoa(name2Id[k])
		bytes = append(bytes, []byte(kId)...)
		bytes = append(bytes, '=')
		bytes = append(bytes, []byte(v)...)
		if it < len(fields)-1 {
			bytes = append(bytes, ';')
		}

		it += 1
	}

	return bytes
}

func (m Batch) encodeWithStartingBuffer(filterCols map[string]struct{}, startingBuf []byte) []byte {
	buf := bytes.NewBuffer(startingBuf)
	first := true

	for _, fieldMap := range m.FieldMaps {
		if !first {
			buf.WriteByte('\n')
		}

		first = false
		encoded := encodeLine(fieldMap, filterCols)
		buf.Write(encoded)
	}

	return buf.Bytes()
}

func (m Batch) Encode(filterCols map[string]struct{}) []byte {
	startingBuf := make([]byte, 1, 1024)
	startingBuf[0] = BATCH
	return m.encodeWithStartingBuffer(filterCols, startingBuf)
}

func (m Batch) EncodeWithQuery(filterCols map[string]struct{}, query int) []byte {
	startingBuf := make([]byte, 2, 1024)
	startingBuf[0] = BATCH
	startingBuf[1] = byte(query)
	return m.encodeWithStartingBuffer(filterCols, startingBuf)
}

func (m Batch) EncodeForPersistance() []byte {
	startingBuf := make([]byte, 0, 1024)
	encoded := m.encodeWithStartingBuffer(nil, startingBuf)
	return append(encoded, "\n"...)
}

func DecodeLine(data []byte) (map[string]string, error) {
	fields := make(map[string]string, 12)

	for kv := range bytes.SplitSeq(data, []byte(";")) {
		pair := bytes.Split(kv, []byte("="))
		if len(pair) != 2 {
			continue
		}

		keyNum, err := strconv.Atoi(string(pair[0]))
		if err != nil || keyNum >= len(id2Name) {
			continue
		}

		if keyNum > len(id2Name) {
			return nil, fmt.Errorf("%v field is not supported by the protocol, must add", err)
		}

		keyName := id2Name[keyNum]
		fields[keyName] = string(pair[1])
	}

	return fields, nil
}

func DecodeBatch(data []byte) (*Batch, error) {
	lines := bytes.Split(data, []byte("\n"))
	fieldMaps := make([]map[string]string, 0, len(lines))

	for _, line := range lines {
		fieldMap, err := DecodeLine(line)
		if err != nil {
			return nil, err
		}

		fieldMaps = append(fieldMaps, fieldMap)
	}

	return &Batch{fieldMaps}, nil
}

type Eof struct{}

func (m Eof) Encode() []byte {
	return []byte{EOF}
}

func (m Eof) EncodeWithQuery(query int) []byte {
	return []byte{EOF, byte(query)}
}

func DecodeEof([]byte) Eof {
	return Eof{}
}

type Error struct{}

func (m Error) Encode() []byte {
	return []byte{ERROR}
}

func (m Error) EncodeWithQuery(query int) []byte {
	return []byte{EOF, byte(query)}
}

func DecodeError([]byte) Error {
	return Error{}
}
