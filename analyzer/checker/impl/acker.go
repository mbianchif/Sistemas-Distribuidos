package impl

import (
	"fmt"
	"net"
	"sync"

	"github.com/op/go-logging"
)

type Acker struct {
	conn             *net.UDPConn
	log              *logging.Logger
	wg               sync.WaitGroup
	keepAliveRetries int
}

func newAcker(port uint16, log *logging.Logger, keepAliveRetries int) (Acker, error) {
	addrStr := fmt.Sprintf(":%d", port)
	addr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		return Acker{}, fmt.Errorf("failed to resolve network address for keep alive: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return Acker{}, fmt.Errorf("failed to open udp connection for keep alive: %v", err)
	}

	return Acker{
		conn:             conn,
		log:              log,
		keepAliveRetries: keepAliveRetries,
	}, nil
}

func SpawnAcker(port uint16, keepAliveRetries int, log *logging.Logger) (*Acker, error) {
	a, err := newAcker(port, log, keepAliveRetries)
	if err != nil {
		return nil, err
	}

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.run()
		log.Infof("Terminating Acker...")
	}()

	return &a, nil
}

func (a *Acker) run() {
	for {
		a.log.Debugf("Waiting for keep-alives")
		_, peerAddr, err := a.conn.ReadFromUDP(nil)
		if err != nil {
			return
		}

		host := peerAddr.IP.String()
		a.log.Debugf("Received a keep-alive from %s, trying to ACK", host)

		for range a.keepAliveRetries {
			_, err = a.conn.WriteToUDP(nil, peerAddr)
			if err == nil {
				break
			}
		}
	}
}

func (a *Acker) Stop() error {
	a.log.Debugf("Waiting for Acker to stop...")

	err := a.conn.Close()
	a.wg.Wait()

	a.log.Debugf("Acker stoped")
	return err
}
