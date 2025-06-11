package middleware

import (
	"analyzer/comms"
)

type Sender interface {
	Batch(comms.Batch, map[string]struct{}, Table) error
	Eof(comms.Eof, Table) error
	Flush(comms.Flush, Table) error
	Purge(comms.Purge, Table) error
	Encode(int) []byte
}
