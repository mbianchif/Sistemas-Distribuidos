package protocol

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"analyzer/comms"
	"analyzer/gateway/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Server struct {
	lis      *CsvTransferListener
	con      *config.Config
	mailer   *SanitizeMailer
	log      *logging.Logger
	recvChan <-chan amqp.Delivery
}

func NewServer(config *config.Config, log *logging.Logger) (*Server, error) {
	mailer, err := NewSanitizeMailer(config, log)
	if err != nil {
		return nil, err
	}

	if err := mailer.Init(); err != nil {
		return nil, err
	}

	lis, err := Bind(config.Host, config.Port, config.Backlog)
	if err != nil {
		return nil, err
	}

	recvChan, err := mailer.Consume()
	if err != nil {
		return nil, err
	}

	return &Server{
		lis:      lis,
		con:      config,
		mailer:   mailer,
		log:      log,
		recvChan: recvChan,
	}, nil
}

func (s *Server) acceptNewConn() *CsvTransferStream {
	s.log.Infof("Waiting for connections...")
	conn, addr := s.lis.Accept()

	if conn == nil {
		return nil
	}

	s.log.Infof("Got a new connection from %v", addr)
	return conn
}

func (s *Server) clientHandler(conn *CsvTransferStream, clientId int) error {
	for range 3 {
		fileName, err := conn.Resource()
		if err != nil {
			return err
		}
		s.log.Infof("Receiving %s", fileName)

		for {
			msg, err := conn.Recv()
			if err != nil {
				return err
			}

			if msg.Kind == MSG_EOF {
				s.log.Infof("%s was successfully received", fileName)
				s.sendEof(fileName, clientId)
				break

			} else if msg.Kind == MSG_BATCH {
				s.sendBatch(fileName, clientId, msg.Data)

			} else if msg.Kind == MSG_ERR {
				s.log.Criticalf("an error was received from the client, exiting...")
				return nil
			} else {
				return fmt.Errorf("an unknown msg kind was received: %d", msg.Kind)
			}
		}
	}

	return nil
}

func (s *Server) sendBatch(fileName string, clientId int, records [][]byte) error {
	body := make([]byte, 0, 24000)
	first := true

	for _, record := range records {
		if !first {
			body = append(body, '\n')
		}

		first = false
		body = append(body, record...)
	}

	return s.mailer.PublishBatch(fileName, clientId, body)
}

func (s *Server) sendEof(fileName string, clientId int) error {
	return s.mailer.PublishEof(fileName, clientId, []byte{})
}

func (s *Server) recvResults(conn *CsvTransferStream) error {
	eofsRecv := 0
	for del := range s.recvChan {
		kind := int(del.Headers["kind"].(int32))
		query := int(del.Headers["query"].(int32))
		clientId := int(del.Headers["client-id"].(int32))
		body := del.Body

		if kind == comms.BATCH {
			batch, err := comms.DecodeBatch(body)
			if err != nil {
				return fmt.Errorf("[%d]: failed to decode batch from query %d", clientId, query)
			}
			result := batch.ToResult(query)
			conn.Send(result)
			s.log.Infof("[%d]: Received batch for query %d: %v", clientId, query, batch)

		} else if kind == MSG_EOF {
			eof := comms.DecodeEof(body)
			result := eof.ToResult(query)
			conn.Send(result)
			s.log.Infof("[%d]: Query %d has been successfully processed", clientId, query)
			eofsRecv += 1

		} else {
			s.log.Errorf("Received an unknown data kind %d", kind)
		}

		del.Ack(false)
		if eofsRecv == 5 {
			fmt.Printf("recibi 5 eofs del cliente %d\n", clientId)
			break
		}
	}

	fmt.Println("termine el loop de recvResults")
	return nil
}

func (s *Server) Run() {
	defer s.mailer.DeInit()

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGTERM)

		<-sigs
		s.log.Info("Received SIGTERM")
		s.lis.Close()
	}()

	for clientId := 0; ; clientId++ {
		conn := s.acceptNewConn()
		if conn == nil {
			break
		}
		defer conn.Close()

		go func() {
			if err := s.clientHandler(conn, clientId); err != nil {
				s.log.Errorf("error while handling client: %v", err)
			}
		}()

		s.recvResults(conn)
	}
}
