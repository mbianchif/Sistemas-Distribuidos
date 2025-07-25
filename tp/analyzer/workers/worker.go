package workers

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	checker "analyzer/checker/impl"
	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/workers/config"

	"github.com/op/go-logging"
)

type IWorker interface {
	Batch(int, middleware.Delivery)
	Eof(int, middleware.Delivery)
	Flush(int, middleware.Delivery)
	Purge(int, middleware.Delivery)
	Close()
}

type Worker struct {
	Log       *logging.Logger
	Mailer    *Mailer
	recvCases []reflect.SelectCase
	con       config.Config
}

func New(con config.Config, log *logging.Logger) (*Worker, error) {
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
			mailer.DeInit()
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

	acker, err := checker.SpawnAcker(base.con.HealthCheckPort, base.con.KeepAliveRetries, base.Log)
	if err != nil {
		return fmt.Errorf("failed to spawn acker: %v", err)
	}
	defer acker.Stop()

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
		case comms.PURGE:
			w.Purge(qId, del)
		default:
			base.Log.Errorf("received an unknown message kind %v", kind)
		}

		base.RussianRoulette("[Process + Send, Dump]")
		clientId := del.Headers.ClientId

		// Dump
		switch kind {
		case comms.BATCH, comms.EOF:
			base.Mailer.Dump(clientId)
		case comms.FLUSH:
			base.Mailer.Flush(clientId)
		case comms.PURGE:
			base.Mailer.Purge()
		default:
			base.Log.Errorf("received an unknown message kind %v", kind)
		}

		base.RussianRoulette("[Dump, Ack]")

		// Ack
		if err := del.Ack(false); err != nil {
			return fmt.Errorf("couldn't acknowledge delivery: %v", err)
		}
	}
}

func (w *Worker) RussianRoulette(format string, args ...any) {
	threshold := w.con.RussianRouletteChance
	r := rand.Float64()

	if r < threshold {
		msg := fmt.Sprintf(format, args...)
		w.Log.Info("Terminated by chance:", msg)
		os.Exit(1)
	}
}

func (w *Worker) Close() {
	w.Mailer.DeInit()
}
