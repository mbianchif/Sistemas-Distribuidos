package protocol

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"analyzer/comms"
	"analyzer/comms/middleware"
	"analyzer/gateway/config"

	"github.com/op/go-logging"
)

type Server struct {
	lis      *CsvTransferListener
	con      *config.Config
	rxMailer *RxMailer
	log      *logging.Logger
	conns    sync.Map
	end      atomic.Bool
	recvChan <-chan middleware.Delivery
}

func NewServer(config *config.Config, log *logging.Logger) (*Server, error) {
	mailer, err := NewRxMailer(config, log)
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
		rxMailer: mailer,
		log:      log,
		conns:    sync.Map{},
		end:      atomic.Bool{},
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
	mailer, err := NewTxMailer(s.con, s.log)
	if err != nil {
		return err
	}

	if err := mailer.Init(); err != nil {
		return err
	}
	defer mailer.DeInit()

	for range 3 {
		fileName, err := conn.Resource()
		if err != nil {
			mailer.PublishFlush(clientId, []byte{})
			s.log.Criticalf("an error ocurred while receiving resource for client %d, exiting...", clientId)
			return err
		}
		s.log.Infof("Receiving %s", fileName)

		for {
			msg, err := conn.Recv()
			if err != nil {
				mailer.PublishFlush(clientId, []byte{})
				s.log.Criticalf("an error ocurred while handling client %d, exiting...", clientId)
				return err
			}

			if msg.Kind == MSG_EOF {
				s.log.Infof("%s was successfully received by client %d", fileName, clientId)
				mailer.PublishEof(fileName, clientId, []byte{})
				break

			} else if msg.Kind == MSG_BATCH {
				mailer.PublishBatch(fileName, clientId, msg.Data)

			} else if msg.Kind == MSG_ERR {
				s.log.Criticalf("an error was received from the client %d, exiting...", clientId)
				return mailer.PublishFlush(clientId, []byte{})

			} else {
				mailer.PublishFlush(clientId, []byte{})
				return fmt.Errorf("an unknown msg kind was received by client %d: %d", clientId, msg.Kind)
			}
		}
	}

	return mailer.PublishFlush(clientId, []byte{})
}

func (s *Server) hasToTerminate() bool {
	if s.end.Load() {
		isEmpty := true
		s.conns.Range(func(_, _ any) bool {
			isEmpty = false
			return false
		})
		return isEmpty
	}
	return false
}

func (s *Server) recvResults() error {
	eofsRecv := make(map[int]int)
	skipped := map[int]struct{}{comms.FLUSH: {}, comms.PURGE: {}}

	for del := range s.recvChan {
		kind := del.Headers.Kind
		query := del.Headers.Query
		clientId := del.Headers.ClientId
		body := del.Body

		if _, ok := skipped[kind]; ok {
			del.Ack(false)
			continue
		}

		connInt, ok := s.conns.Load(clientId)
		if !ok {
			del.Ack(false)
			continue
		}

		conn := connInt.(*CsvTransferStream)
		if _, ok := eofsRecv[clientId]; !ok {
			eofsRecv[clientId] = 0
		}

		if kind == comms.BATCH {
			batch, err := comms.DecodeBatch(body)
			if err != nil {
				del.Ack(false)
				return fmt.Errorf("[%d]: failed to decode batch from query %d", clientId, query)
			}
			result := batch.ToResult(query)
			conn.Send(result)
			s.log.Debugf("[%d]: Received batch for query %d: %v", clientId, query, batch)

		} else if kind == comms.EOF {
			eof := comms.DecodeEof(body)
			result := eof.ToResult(query)
			conn.Send(result)
			s.log.Infof("[%d]: Query %d has been successfully processed", clientId, query)

			eofsRecv[clientId] += 1
			if eofsRecv[clientId] == 5 {
				conn.Close()
				s.conns.Delete(clientId)
				delete(eofsRecv, clientId)
			}

		} else {
			s.log.Errorf("Received an unknown data kind %d", kind)
			conn.Close()
			s.conns.Delete(clientId)
			delete(eofsRecv, clientId)
		}

		del.Ack(false)

		if s.hasToTerminate() {
			s.log.Info("No clients are being attended, exiting...")
			break
		}
	}

	return nil
}

func (s *Server) cleanPipeline() error {
	mailer, err := NewTxMailer(s.con, s.log)
	if err != nil {
		return err
	}

	if err := mailer.Init(); err != nil {
		return err
	}
	defer mailer.DeInit()

	if err := mailer.PublishPurge([]byte{}); err != nil {
		return err
	}

	queryPurges := make(map[int]struct{})
	for del := range s.recvChan {
		kind := del.Headers.Kind
		if kind == comms.PURGE {
			query := del.Headers.Query
			queryPurges[query] = struct{}{}
		}

		del.Ack(false)
		if len(queryPurges) == 5 {
			break
		}
	}

	return s.rxMailer.Purge()
}

func (s *Server) Run() {
	defer s.rxMailer.DeInit()

	if err := s.cleanPipeline(); err != nil {
		s.log.Criticalf("failed to clean pipeline: %v", err)
		return
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGTERM)

		<-sigs
		s.log.Info("Received SIGTERM")
		s.end.Store(true)
		s.lis.Close()
	}()

	go func() {
		if err := s.recvResults(); err != nil {
			s.log.Errorf("error while receiving results: %v", err)
		}
	}()

	for clientId := 0; ; clientId++ {
		conn := s.acceptNewConn()
		if conn == nil {
			break
		}

		s.conns.Store(clientId, conn)
		go func(conn *CsvTransferStream, clientId int) {
			if err := s.clientHandler(conn, clientId); err != nil {
				s.log.Errorf("error while handling client: %v", err)
			}
		}(conn, clientId)
	}
}
