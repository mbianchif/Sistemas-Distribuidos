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

func (c *Client) createClientSocket() error {
	conn, err := BetSockConnect(c.config.ServerAddress)
	if err != nil {
		log.Criticalf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}
	c.conn = conn
    return err
}

func (c *Client) StartClientLoop(betPath string) {
	sigs := make(chan os.Signal, 1)
    defer close(sigs)

	signal.Notify(sigs, syscall.SIGTERM)
	id := c.config.ID

	betFile, err := os.Open(betPath)
	if err != nil {
		log.Criticalf("action: bet_file_open | result: fail | client_id: %v | error: %v", id, err)
	}
	defer betFile.Close()

	if c.createClientSocket() != nil {
        return
    }
	defer c.conn.Close()

	betFileReader := csv.NewReader(betFile)
    bets := make([]Bet, 0)

    for {
        line, err := betFileReader.Read()
        if err != nil {
            break
        }

        bets = append(bets, Bet{
            Agency:    id,
            Name:      line[0],
            Surname:   line[1],
            Id:        line[2],
            Birthdate: line[3],
            Number:    line[4],
        })
    }

    select {
    case _ = <-sigs:
        return
    default:
        err = c.conn.Send(bets, c.config.MaxBatchAmount)
    }

    if err != nil {
        log.Infof("action: send_batch | result: fail | client_id: %v | error: %v", id, err)
    } else {
        log.Infof("action: send_batch | result: success | client_id: %v", id)
	}

    time.Sleep(time.Duration(1) * time.Second)
}
