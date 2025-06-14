package impl

import (
	"analyzer/checker/config"
	"fmt"
	"net"
	"time"

	"github.com/op/go-logging"
)

const SLEEP_DURATION = 10
const KEEP_ALIVE_RETRIES = 5
const FIRST_KEEP_ALIVE_WAIT = 5
const HOST_FMT = "health-checker-%d"

type KeepAlive struct {
	conn      *net.UDPConn
	con       config.Config
	log       *logging.Logger
	peerAddrs map[int]*net.UDPAddr
	peerIds   map[*net.UDPAddr]int
}

func NewKeepAlive(con config.Config, log *logging.Logger) (KeepAlive, error) {
	host := fmt.Sprintf(HOST_FMT, con.Id)
	addrStr := fmt.Sprintf("%s:%d", host, con.KeepAlivePort)
	addr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		return KeepAlive{}, fmt.Errorf("failed to resolve network address for keep alive: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return KeepAlive{}, fmt.Errorf("failed to open udp connection for keep alive: %v", err)
	}

	peerIds := make(map[*net.UDPAddr]int, con.N)
	peerAddrs := make(map[int]*net.UDPAddr, con.N)
	for i := 1; i != con.Id; i = (con.Id + i) % con.N {
		peerHost := fmt.Sprintf(HOST_FMT, i)
		peerAddrStr := fmt.Sprintf("%s:%d", peerHost, con.KeepAlivePort)
		peerAddr, err := net.ResolveUDPAddr("udp", peerAddrStr)
		if err != nil {
			conn.Close()
			return KeepAlive{}, fmt.Errorf("failed to resolve peer network address for keep alive %d: %v", i, err)
		}

		peerIds[peerAddr] = i
		peerAddrs[i] = peerAddr
	}

	return KeepAlive{
		conn:      conn,
		con:       con,
		log:       log,
		peerIds:   peerIds,
		peerAddrs: peerAddrs,
	}, nil
}

func spawnKeepAlive(con config.Config, log *logging.Logger) error {
	ka, err := NewKeepAlive(con, log)
	if err != nil {
		return err
	}

	return ka.Run()

}

func (ka KeepAlive) Direct(id int) time.Time {
	for {
		_, err := ka.conn.WriteToUDP([]byte{}, ka.peerAddrs[id])
		if err == nil {
			return time.Now()
		}
	}
}

func (ka KeepAlive) Broadcast() (map[int]time.Time, error) {
	ts := make(map[int]time.Time, ka.con.N)
	for id := 1; id != ka.con.Id; id = (ka.con.Id + id) % ka.con.N {
		ts[id] = ka.Direct(id)
	}
	return ts, nil
}

func (ka KeepAlive) Wait(ts map[int]time.Time, acks <-chan int) []int {
	// 1. escuchar por el canal en un select, cosa que podamos al mismo
	//    tiempo esperar a que termine un timeout, si se cumple el timeout
	//    todos los ids dentro de ts, estan tarde
	//
	// 2. cuando llega un ack, entonces sacarlo del map ts, volver a esperar
	//
	// 3. Una vez que esperamos RETRIES veces, entonces dar por muerto a los
	//    ids cuales ack no llegaron

	return nil
}

func (ka KeepAlive) Notify(dead []int) error {
	return nil
}

func (ka KeepAlive) Run() error {
	acks := make(chan int)
	go func() {
		for {
			_, peerAddr, err := ka.conn.ReadFromUDP([]byte{})
			if err != nil {
				ka.log.Errorf("failed reading keep-alive message: %v", err)
				continue
			}

			if id, ok := ka.peerIds[peerAddr]; ok {
				acks <- id
			}
		}
	}()

	for {
		ts, err := ka.Broadcast()
		if err != nil {
			return err
		}

		dead := ka.Wait(ts, acks)
		if len(dead) > 0 {
			ka.Notify(dead)
		}

		time.Sleep(time.Duration(SLEEP_DURATION) * time.Second)
	}
}
