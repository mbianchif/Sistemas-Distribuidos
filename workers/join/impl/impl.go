package impl

import (
	"workers"
	"workers/join/config"

	"github.com/op/go-logging"
)

type Join struct {
	*workers.Worker
	Con *config.JoinConfig
}

func New(con *config.JoinConfig, log *logging.Logger) (*Join, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &Join{base, con}, nil
}

func (w *Join) Run() error {
	return w.Worker.Run(w)
}

func (w *Join) Batch(data []byte) bool {
	return false
}

func (w *Join) Eof(data []byte) bool {
	return true
}

func (w *Join) Error(data []byte) bool {
	w.Log.Error("Received an ERROR message kind")
	return true
}
