package rabbit

import (
	"workers/protocol"
)

type Sender interface {
	Batch(protocol.Batch, map[string]struct{}) error
	BatchWithQuery(protocol.Batch, map[string]struct{}, int) error
	Eof(protocol.Eof) error
	Error(protocol.Error) error
}
