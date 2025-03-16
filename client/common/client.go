package common

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
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
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

func (c *Client) StartClientLoop() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		select {
		case _ = <-sigs:
			break
		default:
			c.createClientSocket()
		}

		msg := Message{
			Agency:    c.config.ID,
			Name:      os.Getenv("NOMBRE"),
			Surname:   os.Getenv("APELLIDO"),
			Id:        os.Getenv("DOCUMENTO"),
			Birthdate: os.Getenv("NACIMIENTO"),
			Number:    os.Getenv("NUMERO"),
		}

        log.Infof(fmt.Sprint(msg))
        c.conn.Send(msg)
        log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", msg.Id, msg.Number)

        msg, err := c.conn.Recv()
		c.conn.Close()
		if err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}

		log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
			c.config.ID,
			msg,
		)

		time.Sleep(c.config.LoopPeriod)
	}

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
