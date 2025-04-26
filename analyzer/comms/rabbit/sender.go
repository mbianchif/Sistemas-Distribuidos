package rabbit

import (
	"analyzer/comms"
)

type Sender interface {
	Batch(comms.Batch, map[string]struct{}) error
	BatchWithQuery(comms.Batch, map[string]struct{}, int) error
	Eof(comms.Eof) error
	EofWithQuery(comms.Eof, int) error
	Error(comms.Error) error
}
