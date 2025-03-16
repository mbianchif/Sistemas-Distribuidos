package common

import (
	"encoding/csv"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	MaxBatchAmount int
}

type Client struct {
	config ClientConfig
	conn   *BetSockStream
}

func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

func (c *Client) createClientSocket() {
	conn, err := BetSockConnect(c.config.ServerAddress)
	if err != nil {
		log.Criticalf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}
	c.conn = conn
}

func (c *Client) StartClientLoop(betPath string) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	batchSize := c.config.MaxBatchAmount
	id := c.config.ID

	betFile, err := os.Open(betPath)
	if err != nil {
		log.Criticalf("action: bet_file_open | result: fail | client_id: %v | error: %v", id, err)
	}
	defer betFile.Close()

	betFileReader := csv.NewReader(betFile)
	exit := false

	c.createClientSocket()
	defer c.conn.Close()

	for !exit {
		select {
		case _ = <-sigs:
			break
		default:
		}

		bets := make([]Message, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			line, err := betFileReader.Read()
			if err != nil {
				exit = true
				break
			}

			bets = append(bets, Message{
				Agency:    id,
				Name:      line[0],
				Surname:   line[1],
				Id:        line[2],
				Birthdate: line[3],
				Number:    line[4],
			})
		}

		err := c.conn.Send(bets...)
		if err != nil {
			log.Infof("action send_batch | result: fail | client_id: %v | error: %v", id, err)
            break
		} else {
			log.Infof("action send_batch | result: success | client_id: %v", id)
		}
	}
}
