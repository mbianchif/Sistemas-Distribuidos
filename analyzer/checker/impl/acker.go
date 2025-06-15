package impl

import (
	"fmt"
	"net"
	"sync"

	"github.com/op/go-logging"
)

type Acker struct {
	conn *net.UDPConn
	wg   sync.WaitGroup
}

func newAcker(port uint16) (Acker, error) {
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
		conn: conn,
		wg:   sync.WaitGroup{},
	}, nil
}

func SpawnAcker(port uint16, log *logging.Logger) (*Acker, error) {
	a, err := newAcker(port)
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
		_, peerAddr, err := a.conn.ReadFromUDP([]byte{})
		if err != nil {
			return
		}

		_, err = a.conn.WriteToUDP([]byte{}, peerAddr)
		for err != nil {
			_, err = a.conn.WriteToUDP([]byte{}, peerAddr)
		}
	}
}

func (a *Acker) Stop() error {
	err := a.conn.Close()
	a.wg.Wait()
	return err
}
