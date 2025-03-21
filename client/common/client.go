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

func (c *Client) createClientSocket() {
	conn, err := BetSockConnect(c.config.ServerAddress, c.config.ID)
	if err != nil {
		log.Criticalf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
	}
	c.conn = conn
}

func (c *Client) StartClientLoop(betPath string) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	id := c.config.ID

	betFile, err := os.Open(betPath)
	if err != nil {
		log.Criticalf("action: bet_file_open | result: fail | client_id: %v | error: %v", id, err)
	}
	defer betFile.Close()

	c.createClientSocket()
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
        err = c.conn.SendBets(bets, c.config.MaxBatchAmount)
    }

    if err != nil {
        log.Errorf("action send_batch | result: fail | client_id: %v | error: %v", id, err)
    } else {
        log.Infof("action send_batch | result: success | client_id: %v", id)
	}

    if err = c.conn.Confirm(); err != nil {
        log.Errorf("action confirm_batch | result: fail | client_id: %v | error: %v", id, err)
    } else {
        log.Infof("action confirm_batch | result: success | client_id: %v", id)
    }

    var winners []int
    select {
    case _ = <-sigs:
        return
    default:
        winners, err = c.conn.RecvWinners()
    }

    if err != nil {
        log.Errorf("action: consulta_ganadores | result: fail | error: %v", err)
    } else {
        log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", len(winners))
    }

    time.Sleep(time.Second * time.Duration(10))
}
