package workers

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/workers/config"

	"github.com/op/go-logging"
)

type IWorker interface {
	Batch(int, middleware.Delivery)
	Eof(int, middleware.Delivery)
	Flush(int, middleware.Delivery)
}

type Worker struct {
	Log       *logging.Logger
	Mailer    *Mailer
	recvCases []reflect.SelectCase
	con       *config.Config
}

func New(con *config.Config, log *logging.Logger) (*Worker, error) {
	mailer, err := NewMailer(con, log)
	if err != nil {
		return nil, err
	}

	inputQueues, err := mailer.Init()
	if err != nil {
		return nil, err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sigs)},
	}

	for i, q := range inputQueues {
		ch, err := mailer.Consume(q)
		if err != nil {
			return nil, fmt.Errorf("couldn't start consuming: error with queue %d: %v", i, err)
		}

		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		})
	}

	return &Worker{
		Log:       log,
		Mailer:    mailer,
		recvCases: cases,
		con:       con,
	}, nil
}

func (base *Worker) Run(w IWorker) error {
	base.Log.Infof("Running...")
	cases := base.recvCases

	for {
		qId, value, ok := reflect.Select(cases)
		if !ok {
			return fmt.Errorf("ok in reflective select is false, channel got closed unexpectedly for qId %d", qId)
		}

		if qId == 0 {
			base.Log.Info("received SIGTERM")
			return nil
		}

		base.RussianRoulette("[Recv, Process + Send]")
		del := value.Interface().(middleware.Delivery)
		kind := del.Headers.Kind

		// Process + Send
		switch kind {
		case comms.BATCH:
			w.Batch(qId, del)
		case comms.EOF:
			w.Eof(qId, del)
		case comms.FLUSH:
			w.Flush(qId, del)
		default:
			base.Log.Errorf("received an unknown message type %v", kind)
		}

		// Dump
		base.RussianRoulette("[Process + Send, Dump]")
		clientId := del.Headers.ClientId
		base.Mailer.Dump(clientId)

		// Ack
		base.RussianRoulette("[Dump, Ack]")
		if err := del.Ack(false); err != nil {
			base.Log.Errorf("error while acknowledging message: %v", err)
		}
	}
}

func (w *Worker) RussianRoulette(format string, args ...any) {
	threshold := w.con.RussianRouletteChance
	if threshold == 0 {
		return
	}

	r := int(rand.Uint32()) % 100
	if r < threshold {
		msg := fmt.Sprintf(format, args...)
		w.Log.Info("Terminated by chance: %s", msg)
		os.Exit(1)
	}
}

func (w *Worker) Close() {
	w.Mailer.DeInit()
}
